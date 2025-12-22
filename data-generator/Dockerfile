FROM python:3.9-slim

WORKDIR /app

# Устанавливаем зависимости для clickhouse-driver
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_data.py .

CMD ["python", "generate_data.py"]