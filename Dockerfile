FROM python:3.13-slim

WORKDIR /app

# Install Python dependencies
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Copy API code + config
COPY api/ api/
COPY config/ config/

# Copy data files the API needs (small files only — .dockerignore filters the rest)
# - training metadata (JSON, ~2KB per brand)
# - weekly actions (CSV, ~200KB per brand)
# - size curve alerts (parquet, ~50KB per brand)
COPY models/ models/
COPY weekly_actions/ weekly_actions/
COPY data/processed/ data/processed/

EXPOSE 8080
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
