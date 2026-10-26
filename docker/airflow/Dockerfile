# Self-contained Airflow image: Spark, Java, dbt, GX, and the project CLI.
# The project tree is mounted read-only at runtime; only /opt/venv is baked in.
FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir "uv==0.13.5"

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PYTHONPATH=/opt/taxiflow/src \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1

COPY pyproject.toml uv.lock README.md /opt/taxiflow/
COPY src /opt/taxiflow/src
COPY dbt /opt/taxiflow/dbt
COPY config /opt/taxiflow/config
COPY airflow /opt/taxiflow/airflow

WORKDIR /opt/taxiflow

RUN uv sync --frozen --group quality --group orchestration \
    && chmod -R a+rX /opt/venv /opt/taxiflow \
    && chmod +x /opt/taxiflow/airflow/scripts/*.sh

ENV PATH="/opt/venv/bin:${PATH}"

USER airflow
