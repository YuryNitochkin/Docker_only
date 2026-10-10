FROM python:3.11-slim

# Не создавать .pyc файлы и не буферизовать stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Сначала копируем только requirements, чтобы закешировать слой установки
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Потом копируем код
COPY producer.py .

CMD ["python", "producer.py"]