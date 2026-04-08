FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu AS sparkdist
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.11
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin:/home/airflow/.local/bin"

COPY --from=sparkdist /opt/spark /opt/spark

RUN cd /opt/spark/jars \
    && curl -fsSL -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" -r /requirements.txt
