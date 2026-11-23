# The Chronos Airflow image: Airflow 3.4.0 (Python 3.11) plus what the jobs need to run Spark
# inside a task: Java 17, PySpark 3.5.6 and the Iceberg 1.10.0 runtime jar (docs/decisions.md,
# "Phase 1 runs with a Python runner"; the versions match AWS Glue 5.1 and the local venv).
# Java 17 only: Spark 3.5 doesn't support newer Java. The Airflow image's Debian release has no
# OpenJDK 17 package (only 21), so the JRE is copied from Eclipse Temurin, pinned exactly.
# 17.0.21_9: the newer 17.0.21.1_2 tag has no arm64 build, which Apple Silicon needs.
FROM eclipse-temurin:17.0.21_9-jre-noble AS java

FROM apache/airflow:3.4.0-python3.11

USER root
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# The jar is baked in and checked, so tasks don't download it from Maven on every Spark start.
ARG ICEBERG_JAR=iceberg-spark-runtime-3.5_2.12-1.10.0.jar
ARG ICEBERG_SHA1=e9e56f219ee65d302a96d598470365733b824018
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/${ICEBERG_JAR} \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/${ICEBERG_JAR} \
 && echo "${ICEBERG_SHA1}  /opt/spark-jars/${ICEBERG_JAR}" | sha1sum -c -
ENV CHRONOS_SPARK_JARS=/opt/spark-jars/${ICEBERG_JAR}

USER airflow
# Installed against Airflow's constraints file, so nothing Airflow depends on moves. psycopg is
# already in the image at 3.3.6, the version pyproject.toml pins.
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "pyspark==3.5.6" \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt

# The project code is mounted here by docker-compose.yml, read-only.
ENV PYTHONPATH=/opt/chronos
