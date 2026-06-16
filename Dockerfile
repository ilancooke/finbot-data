FROM python:3.11.15-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY market_data ./market_data
COPY scripts ./scripts

ENTRYPOINT ["python", "scripts/docker_entrypoint.py", "python", "scripts/download_historical_prices.py"]
CMD ["--help"]
