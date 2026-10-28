FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models/hf

WORKDIR /app

# CPU-only PyTorch: far smaller than the default CUDA wheels
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN useradd --create-home --uid 1000 app \
    && mkdir -p /models/hf /data/patches /repos \
    && chown -R app:app /models /data

COPY app ./app

USER app