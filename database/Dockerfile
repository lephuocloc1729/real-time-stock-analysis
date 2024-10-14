FROM python:3.9-slim

WORKDIR /app

COPY database/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY database/ .
COPY config ./config

CMD ["python", "init_db.py"]