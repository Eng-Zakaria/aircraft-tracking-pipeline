FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests

COPY producer.py /app/producer.py

CMD ["python", "/app/producer.py"]
