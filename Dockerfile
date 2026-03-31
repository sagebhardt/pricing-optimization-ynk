FROM python:3.13-slim

WORKDIR /app

# Install Python dependencies
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Copy API code + config + dashboard static files
# ARG busts Docker cache when static assets change
ARG CACHE_BUST=1
COPY api/ api/
COPY config/ config/

# API reads all data from GCS in production (no local data files needed).
# Dirs are created empty so code doesn't error on path checks.
RUN mkdir -p models weekly_actions data/processed

EXPOSE 8080
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
