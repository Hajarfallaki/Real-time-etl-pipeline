FROM apache/spark:4.0.0

USER root

WORKDIR /app

COPY streaming_job.py /app/streaming_job.py

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "/app/streaming_job.py"]