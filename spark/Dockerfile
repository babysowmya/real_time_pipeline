FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y curl

# Postgres JDBC
RUN curl -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app
COPY ./app /app

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/streaming_app.py"]