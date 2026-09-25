FROM python:3.11-slim

# Install system dependencies & Java runtime for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency requirements first for build caching
COPY requirements.txt .

# Increase default timeout to 1000s and add retries for large package downloads (PySpark)
RUN pip install --no-cache-dir --default-timeout=1000 --retries=10 -r requirements.txt

# Copy repository code
COPY . .

# Expose FastAPI port
EXPOSE 8000

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]