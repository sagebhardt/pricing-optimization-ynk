FROM python:3.13-slim

WORKDIR /app

# Install system dependencies for XGBoost
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Copy application code + bundled dashboard
COPY api/ api/
COPY src/ src/
COPY config/ config/
COPY models/ models/
COPY data/processed/ data/processed/
COPY data/raw/ data/raw/
COPY weekly_actions/ weekly_actions/

# Expose port
EXPOSE 8080

# Run with uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
