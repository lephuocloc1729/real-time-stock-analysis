# kafka/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY kafka/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/ .
COPY config ./config

CMD ["python", "producer.py"]