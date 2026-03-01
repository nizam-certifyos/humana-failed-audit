# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependency list first (Docker caches this layer if unchanged)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY . .

# Cloud Run sets PORT automatically (default 8080)
ENV PORT=8080

# Gunicorn: 1 worker, 8 threads, 3600s timeout (audit can run ~40 min)
CMD ["gunicorn", \
     "--bind", "0.0.0.0:8080", \
     "--workers", "1", \
     "--threads", "8", \
     "--timeout", "3600", \
     "--access-logfile", "-", \
     "main:app"]
