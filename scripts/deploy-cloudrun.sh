#!/usr/bin/env bash
# Deploy ke Google Cloud Run (gratis tier).
#
# Prasyarat:
#   - gcloud CLI terinstall & login:  gcloud auth login
#   - project aktif:                   gcloud config set project <PROJECT_ID>
#   - billing account terpasang (kuota gratis tetap gratis)
#
# Database & Redis gratis yang disarankan:
#   - Neon Postgres (free 0.5GB):  https://neon.tech
#   - Upstash Redis  (free):       https://upstash.com
#
# Contoh:
#   DATABASE_URL="postgresql://user:pass@host/db?sslmode=require" \
#   REDIS_ADDR="eu1-xxx.upstash.io:6379" REDIS_PASSWORD="xxx" \
#   ./scripts/deploy-cloudrun.sh
set -euo pipefail

SERVICE=${SERVICE:-trading-price-api}
REGION=${REGION:-asia-southeast2}
PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}

if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  echo "ERROR: set project dulu: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

: "${DATABASE_URL:?set DATABASE_URL (Neon/Cloud SQL)}"
REDIS_ADDR=${REDIS_ADDR:-""}
REDIS_PASSWORD=${REDIS_PASSWORD:-""}

# 58 simbol default (sama dengan symbols.txt)
DEFAULT_SYMBOLS=${DEFAULT_SYMBOLS:-"FOREXCOM:XAUUSD,FOREXCOM:XAGUSD,FOREXCOM:EURUSD"}

ENV_VARS="DATABASE_URL=${DATABASE_URL},RUN_MIGRATIONS=true"
ENV_VARS="${ENV_VARS},UPSTREAM_WS_URL=wss://data.tradingview.com/socket.io/websocket"
ENV_VARS="${ENV_VARS},UPSTREAM_WS_ORIGIN=https://data.tradingview.com"
ENV_VARS="${ENV_VARS},UPSTREAM_SCANNER_URL=https://scanner.tradingview.com"
ENV_VARS="${ENV_VARS},DEFAULT_SYMBOLS=${DEFAULT_SYMBOLS}"
ENV_VARS="${ENV_VARS},TICK_RETENTION=24 hours"
ENV_VARS="${ENV_VARS},CANDLE_RETENTION=720 hours"
if [[ -n "$REDIS_ADDR" ]]; then
  ENV_VARS="${ENV_VARS},REDIS_ADDR=${REDIS_ADDR}"
fi
if [[ -n "$REDIS_PASSWORD" ]]; then
  ENV_VARS="${ENV_VARS},REDIS_PASSWORD=${REDIS_PASSWORD}"
fi

echo "==> Deploy ${SERVICE} ke ${REGION} (project ${PROJECT_ID})"
gcloud run deploy "$SERVICE" \
  --source . \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --allow-unauthenticated \
  --cpu 1 \
  --memory 512Mi \
  --min-instances 0 \
  --max-instances 3 \
  --timeout 600 \
  --concurrency 200 \
  --set-env-vars "$ENV_VARS" \
  --port 8080

echo "==> Selesai. URL:"
gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)'