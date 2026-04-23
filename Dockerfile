FROM apache/airflow:2.9.2-python3.11

USER root

# ── System deps: Java (required by PySpark) ──────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Detect arch-specific JVM path (amd64 on Intel, arm64 on Apple Silicon)
RUN JAVA_HOME=$(find /usr/lib/jvm -maxdepth 1 -name "java-17-openjdk-*" -type d | head -1) && \
    echo "JAVA_HOME=$JAVA_HOME" && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    ln -sf "$JAVA_HOME" /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── PostgreSQL JDBC driver (PySpark uses this to write to Postgres) ───────────
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/postgresql-42.7.3.jar pyspark-shell"

USER airflow

# ── Python packages ───────────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── dbt project files ─────────────────────────────────────────────────────────
ENV DBT_PROJECT_DIR=/opt/airflow/dbt_project
