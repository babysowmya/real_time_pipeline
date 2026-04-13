FROM apache/spark:3.5.1

WORKDIR /app

COPY app/ /app/

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "--conf", "spark.jars.ivy=/tmp/.ivy2", "/app/streaming_app.py"]