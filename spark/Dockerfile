
FROM docker.io/apache/spark:3.5.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    influxdb-client==1.43.0 \
    pyspark==3.5.1

# App code
COPY streaming_job.py /app/streaming_job.py

WORKDIR /app

CMD ["spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/streaming_job.py"]
