#!/bin/bash
# Weekly pricing pipeline — runs every Monday morning
# Regenerates pricing actions for all active brands
#
# Usage:
#   ./run_weekly.sh                    # Run all brands
#   ./run_weekly.sh BAMERS OAKLEY      # Run specific brands
#
# Cron example (every Monday at 6:00 AM Chile time):
#   0 6 * * 1 cd /Users/sebage/Documents/GitHub/pricing-optimization-ynk && ./run_weekly.sh >> logs/weekly_$(date +\%F).log 2>&1

set -euo pipefail
cd "$(dirname "$0")"

# All active brands
ALL_BRANDS=(HOKA BOLD BAMERS OAKLEY)
BRANDS=("${@:-${ALL_BRANDS[@]}}")

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%F_%H%M)

echo "============================================================"
echo "WEEKLY PRICING RUN — $(date)"
echo "Brands: ${BRANDS[*]}"
echo "============================================================"

FAILED=()

for BRAND in "${BRANDS[@]}"; do
    echo ""
    echo "--- $BRAND ---"
    BRAND_LOG="$LOG_DIR/${BRAND,,}_${TIMESTAMP}.log"

    if python3 run_brand.py "$BRAND" --steps extract features enhance aggregate train pricing > "$BRAND_LOG" 2>&1; then
        ACTIONS_FILE=$(ls -t weekly_actions/${BRAND,,}/pricing_actions_*.csv 2>/dev/null | head -1)
        if [ -n "$ACTIONS_FILE" ]; then
            N_ACTIONS=$(tail -n +2 "$ACTIONS_FILE" | wc -l | tr -d ' ')
            echo "  OK: $N_ACTIONS actions generated -> $ACTIONS_FILE"
        else
            echo "  OK: No actions this week"
        fi
    else
        echo "  FAILED — see $BRAND_LOG"
        FAILED+=("$BRAND")
    fi
done

# Deploy if any brand succeeded
if [ ${#FAILED[@]} -lt ${#BRANDS[@]} ]; then
    echo ""
    echo "--- DEPLOYING ---"
    cd dashboard && npx vite build --outDir ../api/static --emptyOutDir > /dev/null 2>&1 && cd ..
    gcloud builds submit . \
        --tag us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api \
        --project ynk-pricing-optimization \
        --quiet > "$LOG_DIR/deploy_${TIMESTAMP}.log" 2>&1
    gcloud run deploy pricing-api \
        --image us-central1-docker.pkg.dev/ynk-pricing-optimization/ynk-docker/pricing-api \
        --region us-central1 --project ynk-pricing-optimization --platform managed \
        --memory 2Gi --cpu 2 --allow-unauthenticated \
        --set-env-vars "PYTHONPATH=/app,GCS_BUCKET=ynk-pricing-decisions,GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID" \
        --quiet >> "$LOG_DIR/deploy_${TIMESTAMP}.log" 2>&1
    echo "  Deployed to Cloud Run"
fi

echo ""
echo "============================================================"
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "DONE with errors: ${FAILED[*]} failed"
    exit 1
else
    echo "DONE — all brands updated and deployed"
fi
