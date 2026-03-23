#!/bin/bash
# Deploy HOKA Markdown Optimization API to Google Cloud Run
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - Project ID set
#   - Artifact Registry repository created
#
# Usage: ./deploy.sh [PROJECT_ID] [REGION]

PROJECT_ID=${1:-"ynk-ai"}
REGION=${2:-"us-central1"}
SERVICE_NAME="hoka-pricing-api"
REPO="ynk-docker"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}"

echo "=== HOKA Pricing API Deployment ==="
echo "Project: ${PROJECT_ID}"
echo "Region:  ${REGION}"
echo "Image:   ${IMAGE}"
echo ""

# Build
echo "Building Docker image..."
gcloud builds submit --tag "${IMAGE}" --project "${PROJECT_ID}"

# Deploy
echo "Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
    --image "${IMAGE}" \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --platform managed \
    --memory 2Gi \
    --cpu 2 \
    --min-instances 0 \
    --max-instances 3 \
    --timeout 60 \
    --allow-unauthenticated \
    --set-env-vars "PYTHONPATH=/app"

echo ""
echo "=== Deployment complete ==="
gcloud run services describe "${SERVICE_NAME}" --region "${REGION}" --project "${PROJECT_ID}" --format="value(status.url)"
