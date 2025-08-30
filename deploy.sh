#!/usr/bin/env bash
set -Eeuo pipefail

### ===== CONFIG (runs from /root/auth/auth_monitor_ingestion) =====
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PROD_DIR="/srv/auth_monitor_ingestion"
VENV_BIN="/root/.pyenv/versions/venv_auth_monitor_ingestion/bin"
WHEELS_DIR="$PROD_DIR/wheels_linux"

HEALTH_URL="https://127.0.0.1:8443/health"
TEST_URL="https://127.0.0.1:8443/v1/ingest/test"

# Optional: write a test row after deploy (0=off, 1=on)
SMOKE_INGEST=0
SMOKE_SERIAL="SNDEPLOY0000000001"
SMOKE_PROTOCOL="rps"
SMOKE_LOCATION="Deploy Room"
SMOKE_TOKEN="tok-deploy-smoke"

### ===== helpers =====
STEP=0
step() { printf "\n=== [%02d] %s ===\n" "$((++STEP))" "$*"; }
die() { echo "❌ $*" >&2; exit 1; }

start_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
SENT_AT="$start_ts"
TOKEN_TS="$start_ts"

### ===== 1) Sync work tree -> /srv =====
step "Sync work tree → $PROD_DIR"
mkdir -p "$PROD_DIR"
rsync -av --delete \
  --exclude=".git" --exclude="__pycache__" --exclude=".idea" \
  --exclude="venv_*" \
  "$REPO_DIR"/ "$PROD_DIR"/

# If /srv/.env is missing but you have a local .env, copy it once.
if [[ ! -f "$PROD_DIR/.env" && -f "$REPO_DIR/.env" ]]; then
  step "First-time .env copy to /srv (since it was missing)"
  cp "$REPO_DIR/.env" "$PROD_DIR/.env"
fi

### ===== 2) Ensure .env exists in /srv =====
step "Check /srv .env"
[[ -f "$PROD_DIR/.env" ]] || die "Missing $PROD_DIR/.env (create it once or place a .env beside this script for first-time copy)"

### ===== 3) Install/update dependencies into pyenv venv =====
step "Install/Update dependencies"
"$VENV_BIN/python" -V >/dev/null 2>&1 || die "Python venv not found at $VENV_BIN"
if [[ -d "$WHEELS_DIR" ]]; then
  echo "Using local wheels: $WHEELS_DIR"
  "$VENV_BIN/python" -m pip install --no-input --no-index --find-links="$WHEELS_DIR" -r "$PROD_DIR/requirements.txt"
else
  echo "Local wheels not found; using index"
  "$VENV_BIN/python" -m pip install --no-input -r "$PROD_DIR/requirements.txt"
fi

### ===== 4) Reload & restart services =====
step "systemd daemon-reload"
systemctl daemon-reload

step "Restart ingest-api and ingest-worker"
systemctl restart ingest-api
systemctl restart ingest-worker

### ===== 5) Health check via nginx (8443) =====
step "Health check"
for i in {1..20}; do
  if curl -fsSk "$HEALTH_URL" >/dev/null; then break; fi
  sleep 0.5
done
echo "Health response:"
curl -sSk "$HEALTH_URL" || die "Health check failed"

### ===== 6) Dry-run ping =====
step "Dry-run /v1/ingest/test"
PING_PAYLOAD=$(cat <<JSON
{"schema_version":1,"sent_at":"$SENT_AT","client_request_id":"deploy-ping","items":[]}
JSON
)
curl -sSk -H 'Content-Type: application/json' -d "$PING_PAYLOAD" "$TEST_URL" || die "Dry-run failed"

### ===== 7) Optional smoke ingest =====
if [[ "$SMOKE_INGEST" -eq 1 ]]; then
  step "Smoke ingest (1 item) — writes a test row"
  SMOKE_PAYLOAD=$(cat <<JSON
{
  "schema_version": 1,
  "sent_at": "$SENT_AT",
  "client_request_id": "deploy-smoke",
  "items": [{
    "serial_number": "$SMOKE_SERIAL",
    "location": "$SMOKE_LOCATION",
    "protocol_type": "$SMOKE_PROTOCOL",
    "token": "$SMOKE_TOKEN",
    "token_created_at": "$TOKEN_TS"
  }]}
JSON
)
  curl -sSk -H 'Content-Type: application/json' -d "$SMOKE_PAYLOAD" https://127.0.0.1:8443/v1/ingest \
    || die "Smoke ingest failed"
fi

### ===== 8) Show recent logs =====
step "Recent logs since $start_ts"
echo "-- ingest-api --"
journalctl -u ingest-api --since "$start_ts" --no-pager -n 80 || true
echo "-- ingest-worker --"
journalctl -u ingest-worker --since "$start_ts" --no-pager -n 80 || true

echo -e "\n✅ Deployment complete."
