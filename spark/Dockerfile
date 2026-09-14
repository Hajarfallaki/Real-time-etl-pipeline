FROM apache/spark:4.0.0

USER root

WORKDIR /app

COPY streaming_job.py /app/streaming_job.py
COPY transformation.py /app/transformation.py
COPY load.py /app/load.py

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.postgresql:postgresql:42.7.4", "/app/streaming_job.py"]