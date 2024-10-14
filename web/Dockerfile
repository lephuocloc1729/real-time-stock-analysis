# web/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY web/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY web/ .
COPY config ./config

CMD ["python", "app.py"]