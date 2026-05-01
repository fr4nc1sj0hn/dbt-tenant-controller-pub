FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install \
    --default-timeout=300 \
    --retries=10 \
    -r requirements.txt

# Copy app
COPY app/ .


# Run FastAPI with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
