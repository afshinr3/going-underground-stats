FROM mcr.microsoft.com/playwright/python:v1.63.0-jammy
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1 TEMP_DIR=/app/temp DB_PATH=/app/data/state.sqlite3
CMD ["python", "main.py"]
