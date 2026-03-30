FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  CHROME_BIN=/usr/bin/chromium

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
  chromium \
  chromium-driver \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 5000

CMD ["python", "app.py"]
