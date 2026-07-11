#!/usr/bin/env bash
# Deploy Meta Ads medallion example to a Microsoft Fabric workspace.
#
# Prerequisites:
#   - Azure CLI (`az`)
#   - jq, curl, python3
#   - Fabric capacity assigned to the target workspace
#
# Usage:
#   export FABRIC_WORKSPACE_NAME="MetaAds-Dev"
#   export FABRIC_LAKEHOUSE_NAME="meta_ads_lh"
#   export META_CSV_DIR="/path/to/meta/csvs"   # your 6 CSV files
#   ./scripts/deploy_to_fabric.sh
#
# Optional:
#   export FABRIC_CAPACITY_ID="<capacity-guid>"  # required if creating a new workspace
#   export SKIP_RUN=1                           # upload only; do not execute notebooks

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WS_NAME="${FABRIC_WORKSPACE_NAME:?Set FABRIC_WORKSPACE_NAME}"
LH_NAME="${FABRIC_LAKEHOUSE_NAME:-meta_ads_lh}"
CSV_DIR="${META_CSV_DIR:?Set META_CSV_DIR to folder with Meta CSVs}"
CAPACITY_ID="${FABRIC_CAPACITY_ID:-}"
SKIP_RUN="${SKIP_RUN:-0}"
API="https://api.fabric.microsoft.com"
ONELAKE="https://onelake.dfs.fabric.microsoft.com"

need() { command -v "$1" >/dev/null || { echo "Missing dependency: $1"; exit 1; }; }
need az; need jq; need curl; need python3; need base64

echo "== Auth =="
az account show >/dev/null 2>&1 || az login --allow-no-subscriptions
FABRIC_TOKEN=$(az account get-access-token --resource "$API" --query accessToken -o tsv)
STORAGE_TOKEN=$(az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv)
AUTH_F=(-H "Authorization: Bearer $FABRIC_TOKEN" -H "Content-Type: application/json")
AUTH_S=(-H "Authorization: Bearer $STORAGE_TOKEN")

echo "== Resolve / create workspace: $WS_NAME =="
WS_ID=$(az rest --method get --resource "$API" \
  --url "$API/v1/workspaces" \
  --query "value[?displayName=='$WS_NAME'].id | [0]" -o tsv)

if [[ -z "${WS_ID:-}" || "$WS_ID" == "null" ]]; then
  [[ -n "$CAPACITY_ID" ]] || { echo "Workspace not found. Set FABRIC_CAPACITY_ID to create it."; exit 1; }
  BODY=$(jq -n --arg n "$WS_NAME" --arg c "$CAPACITY_ID" '{displayName:$n, capacityId:$c}')
  WS_ID=$(az rest --method post --resource "$API" \
    --url "$API/v1/workspaces" --body "$BODY" --query id -o tsv)
  echo "Created workspace $WS_ID"
else
  echo "Using workspace $WS_ID"
fi

echo "== Resolve / create lakehouse: $LH_NAME =="
LH_ID=$(az rest --method get --resource "$API" \
  --url "$API/v1/workspaces/$WS_ID/items?type=Lakehouse" \
  --query "value[?displayName=='$LH_NAME'].id | [0]" -o tsv)

if [[ -z "${LH_ID:-}" || "$LH_ID" == "null" ]]; then
  BODY=$(jq -n --arg n "$LH_NAME" '{displayName:$n, type:"Lakehouse", creationPayload:{enableSchemas:true}}')
  LH_ID=$(az rest --method post --resource "$API" \
    --url "$API/v1/workspaces/$WS_ID/items" --body "$BODY" --query id -o tsv)
  echo "Created lakehouse $LH_ID (waiting for provision...)"
  sleep 15
else
  echo "Using lakehouse $LH_ID"
fi

# Prefer workspace/lakehouse display names for OneLake DFS paths
WS_ONELAKE_NAME=$(az rest --method get --resource "$API" \
  --url "$API/v1/workspaces/$WS_ID" --query displayName -o tsv)

echo "== Upload CSVs to Files/landing/meta =="
# Create directories
for DIR in "Files" "Files/landing" "Files/landing/meta"; do
  curl -s -X PUT "$ONELAKE/$WS_ONELAKE_NAME/$LH_NAME/$DIR?resource=directory" \
    "${AUTH_S[@]}" -H "Content-Length: 0" >/dev/null || true
done

shopt -s nullglob
FILES=("$CSV_DIR"/meta_*.csv)
[[ ${#FILES[@]} -gt 0 ]] || { echo "No meta_*.csv found in $CSV_DIR"; exit 1; }

for f in "${FILES[@]}"; do
  name=$(basename "$f")
  size=$(wc -c <"$f" | tr -d ' ')
  echo "  uploading $name ($size bytes)"
  curl -s -X PUT \
    "$ONELAKE/$WS_ONELAKE_NAME/$LH_NAME/Files/landing/meta/$name?resource=file" \
    "${AUTH_S[@]}" -H "Content-Length: 0" >/dev/null
  curl -s -X PATCH \
    "$ONELAKE/$WS_ONELAKE_NAME/$LH_NAME/Files/landing/meta/$name?action=append&position=0" \
    "${AUTH_S[@]}" -H "Content-Type: application/octet-stream" \
    --data-binary @"$f" >/dev/null
  curl -s -X PATCH \
    "$ONELAKE/$WS_ONELAKE_NAME/$LH_NAME/Files/landing/meta/$name?action=flush&position=$size" \
    "${AUTH_S[@]}" >/dev/null
done

encode_notebook() {
  # Fabric expects base64 of the .ipynb JSON
  python3 - <<'PY' "$1"
import base64, pathlib, sys
print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode())
PY
}

upsert_notebook() {
  local nb_path="$1"
  local nb_name="$2"
  local payload
  payload=$(encode_notebook "$nb_path")

  local existing
  existing=$(az rest --method get --resource "$API" \
    --url "$API/v1/workspaces/$WS_ID/items?type=Notebook" \
    --query "value[?displayName=='$nb_name'].id | [0]" -o tsv)

  local def
  def=$(jq -n \
    --arg payload "$payload" \
    --arg lh_id "$LH_ID" \
    --arg lh_name "$LH_NAME" \
    --arg ws_id "$WS_ID" \
    '{
      definition: {
        format: "ipynb",
        parts: [
          { path: "notebook-content.ipynb", payload: $payload, encoding: "base64" }
        ]
      },
      updateMetadata: true,
      dependencies: {
        lakehouse: {
          default_lakehouse: $lh_id,
          default_lakehouse_name: $lh_name,
          default_lakehouse_workspace_id: $ws_id
        }
      }
    }')

  # Note: lakehouse binding is also embedded via notebook metadata when supported.
  # Create item first if needed, then updateDefinition.
  local nb_id="$existing"
  if [[ -z "${nb_id:-}" || "$nb_id" == "null" ]]; then
    nb_id=$(az rest --method post --resource "$API" \
      --url "$API/v1/workspaces/$WS_ID/items" \
      --body "$(jq -n --arg n "$nb_name" '{displayName:$n, type:"Notebook"}')" \
      --query id -o tsv)
    echo "Created notebook $nb_name ($nb_id)"
  else
    echo "Updating notebook $nb_name ($nb_id)"
  fi

  # Build updateDefinition body (content + lakehouse metadata in ipynb)
  # Inject lakehouse metadata into notebook before encode for reliable binding
  local tmp
  tmp=$(mktemp)
  python3 - <<PY
import json, pathlib
nb = json.loads(pathlib.Path(r"$nb_path").read_text())
nb.setdefault("metadata", {})
nb["metadata"]["dependencies"] = {
  "lakehouse": {
    "default_lakehouse": "$LH_ID",
    "default_lakehouse_name": "$LH_NAME",
    "default_lakehouse_workspace_id": "$WS_ID"
  }
}
pathlib.Path(r"$tmp").write_text(json.dumps(nb))
PY
  local b64
  b64=$(encode_notebook "$tmp")
  rm -f "$tmp"

  local body
  body=$(jq -n --arg p "$b64" '{
    definition: {
      format: "ipynb",
      parts: [ {path:"notebook-content.ipynb", payload:$p, encoding:"base64"} ]
    }
  }')

  OP=$(az rest --method post --resource "$API" \
    --url "$API/v1/workspaces/$WS_ID/items/$nb_id/updateDefinition" \
    --body "$body" -o json)
  # LRO may return operation URL via headers; az rest may not expose them.
  # Poll briefly by re-get; Succeeded is typical for small notebooks.
  sleep 5
  echo "$nb_id"
}

echo "== Deploy notebooks =="
NB1=$(upsert_notebook "$ROOT/notebooks/01_bronze_ingest.ipynb" "meta_ads_01_bronze")
NB2=$(upsert_notebook "$ROOT/notebooks/02_silver_transform.ipynb" "meta_ads_02_silver")
NB3=$(upsert_notebook "$ROOT/notebooks/03_gold_star_schema.ipynb" "meta_ads_03_gold")

run_notebook() {
  local nb_id="$1"
  local label="$2"
  echo "== Run $label =="
  # Avoid duplicate runs
  RECENT=$(az rest --method get --resource "$API" \
    --url "$API/v1/workspaces/$WS_ID/items/$nb_id/jobs/instances?continuationToken=" \
    --query "value[0].status" -o tsv 2>/dev/null || true)

  JOB_BODY=$(jq -n --arg id "$LH_ID" --arg name "$LH_NAME" '{
    executionData: {
      configuration: {
        useStarterPool: true,
        defaultLakehouse: { id: $id, name: $name }
      }
    }
  }')

  az rest --method post --resource "$API" \
    --url "$API/v1/workspaces/$WS_ID/items/$nb_id/jobs/instances?jobType=RunNotebook" \
    --body "$JOB_BODY" >/tmp/fabric_job_$label.json || true

  # Poll up to ~30 minutes
  for i in $(seq 1 60); do
    STATUS=$(az rest --method get --resource "$API" \
      --url "$API/v1/workspaces/$WS_ID/items/$nb_id/jobs/instances" \
      --query "value[0].status" -o tsv)
    echo "  [$label] status=$STATUS"
    case "$STATUS" in
      Completed) return 0 ;;
      Failed|Cancelled|Deduped) echo "Job ended: $STATUS"; return 1 ;;
    esac
    sleep 30
  done
  echo "Timed out waiting for $label"; return 1
}

if [[ "$SKIP_RUN" != "1" ]]; then
  run_notebook "$NB1" "bronze"
  run_notebook "$NB2" "silver"
  run_notebook "$NB3" "gold"
  echo
  echo "Done. In Fabric portal open lakehouse '$LH_NAME' and inspect schemas bronze/silver/gold."
  echo "Reporting table: gold.rpt_meta_ad_performance_daily"
  echo "SQL view:        gold.vw_meta_ad_performance"
else
  echo "SKIP_RUN=1 — notebooks uploaded but not executed."
fi

echo
echo "Workspace: $WS_NAME ($WS_ID)"
echo "Lakehouse: $LH_NAME ($LH_ID)"
