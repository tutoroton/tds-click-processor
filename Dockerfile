FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ ./app/

# Run with uvicorn.
# F.41 — worker count is env-driven (WEB_CONCURRENCY, default 2 = unchanged).
# device_detector is GIL-bound CPU work, so real parse concurrency scales with
# PROCESSES, not threads; WEB_CONCURRENCY lets ops size it to the node's cores/RAM
# without a rebuild (each worker holds its own ~20MB device_detector + UA LRU —
# size against droplet RAM before raising). Shell form with `exec` so ${VAR}
# expands AND uvicorn still becomes PID 1 (receives SIGTERM directly → the
# lifespan graceful shutdown / background-task cancellation is preserved).
CMD ["/bin/sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8100 --workers ${WEB_CONCURRENCY:-2} --log-level info"]

EXPOSE 8100

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')"
