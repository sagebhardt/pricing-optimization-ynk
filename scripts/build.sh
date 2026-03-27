#!/usr/bin/env bash
set -euo pipefail

# Build and optionally deploy pricing-api or pricing-pipeline images.
# Usage:
#   ./scripts/build.sh api              # build API image
#   ./scripts/build.sh pipeline         # build pipeline image (safe swap)
#   ./scripts/build.sh api --deploy     # build + deploy API to Cloud Run
#   ./scripts/build.sh pipeline --deploy # build + update Cloud Run Job

PROJECT="ynk-pricing-optimization"
REGION="us-central1"
REPO="us-central1-docker.pkg.dev/$PROJECT/ynk-docker"

TARGET="${1:-}"
DEPLOY="${2:-}"

if [[ -z "$TARGET" || ! "$TARGET" =~ ^(api|pipeline)$ ]]; then
    echo "Usage: $0 <api|pipeline> [--deploy]"
    echo ""
    echo "  api       Build slim API image (~50MB)"
    echo "  pipeline  Build full pipeline image (ML + DB deps)"
    echo "  --deploy  Also deploy to Cloud Run after building"
    exit 1
fi

cd "$(git rev-parse --show-toplevel)"

build_api() {
    local IMAGE="$REPO/pricing-api"
    echo "==> Building API image: $IMAGE"
    gcloud builds submit . --tag "$IMAGE" --project "$PROJECT"

    if [[ "$DEPLOY" == "--deploy" ]]; then
        echo "==> Deploying API to Cloud Run..."
        gcloud run deploy pricing-api \
            --image "$IMAGE" \
            --region "$REGION" \
            --project "$PROJECT" \
            --platform managed \
            --memory 512Mi --cpu 1 \
            --min-instances 1 \
            --allow-unauthenticated \
            --set-env-vars "PYTHONPATH=/app,GCS_BUCKET=ynk-pricing-decisions,GOOGLE_CLIENT_ID=467343668842-b1imqgobg3l6v6670tnir2nsis5pv56v.apps.googleusercontent.com"
        echo "==> API deployed."
    fi
}

build_pipeline() {
    local IMAGE="$REPO/pricing-pipeline"

    # Verify we're starting from API state (safety check)
    if grep -q "Dockerfile.pipeline" Dockerfile 2>/dev/null; then
        echo "ERROR: Dockerfile already looks like pipeline. Aborting."
        echo "Restore API Dockerfile first: git checkout Dockerfile .dockerignore"
        exit 1
    fi

    # Swap Dockerfile + .dockerignore for pipeline build, restore on ANY exit
    cp Dockerfile Dockerfile._api_backup
    cp .dockerignore .dockerignore._api_backup
    trap 'mv Dockerfile._api_backup Dockerfile; mv .dockerignore._api_backup .dockerignore; echo "==> Restored API Dockerfile + .dockerignore"' EXIT

    cp Dockerfile.pipeline Dockerfile
    cp .dockerignore.pipeline .dockerignore

    echo "==> Building pipeline image: $IMAGE"
    gcloud builds submit . --tag "$IMAGE" --project "$PROJECT"

    if [[ "$DEPLOY" == "--deploy" ]]; then
        echo "==> Updating Cloud Run Job..."
        gcloud run jobs update pricing-pipeline \
            --image "$IMAGE" \
            --region "$REGION" \
            --project "$PROJECT"
        echo "==> Pipeline job updated."
    fi

    # trap handles restoration
}

case "$TARGET" in
    api)      build_api ;;
    pipeline) build_pipeline ;;
esac

echo "==> Done."
