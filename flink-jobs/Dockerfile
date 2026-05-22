# Custom PyFlink image: Flink 1.18 + Python + Kafka connector + S3/MinIO plugin
# Built locally and loaded into minikube via `eval $(minikube docker-env)`
FROM flink:1.18-scala_2.12-java11

ARG FLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG HADOOP_VERSION=3.3.4

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ── Python runtime ────────────────────────────────────────────────────────
USER root
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── Kafka connector (in /opt/flink/lib so the JM/TM classpath picks it up) ─
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# ── S3 filesystem plugin for MinIO (must live in plugins/, not lib/) ───────
# Use hadoop variant — it implements RecoverableWriter, which FileSink (Sink2 API) requires.
# The presto variant does not implement RecoverableWriter and causes
# "Could not create committable serializer" at graph-build time.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar /opt/flink/plugins/s3-fs-hadoop/

# ── Symlink the Python driver jar to a stable name under a separate dir.
#    The original MUST stay in /opt/flink/opt/ — PackagedProgramUtils.getPythonJar()
#    scans that directory and requires exactly one flink-python*.jar there.
#    The jarURI in FlinkDeployment points here for a version-independent path.
RUN mkdir -p /opt/flink/python-driver && \
    ln -sf /opt/flink/opt/flink-python-${FLINK_VERSION}.jar /opt/flink/python-driver/flink-python.jar

# ── flink-conf overrides for MinIO and Kubernetes service account ─────────
RUN { \
      echo ""; \
      echo "# MinIO via S3 plugin"; \
      echo "s3.endpoint: http://minio.stockage.svc:9000"; \
      echo "s3.path.style.access: true"; \
      echo "s3.access-key: minioadmin"; \
      echo "s3.secret-key: minioadmin"; \
      echo ""; \
      echo "# Checkpoints / state on local disk for the POC"; \
      echo "state.backend: hashmap"; \
      echo "state.checkpoints.dir: file:///tmp/flink-checkpoints"; \
    } >> /opt/flink/conf/flink-conf.yaml

# ── Job code ──────────────────────────────────────────────────────────────
WORKDIR /opt/jobs
COPY common /opt/jobs/common
COPY job0_cdc_adapter.py job1_decryption.py job2_validation.py job3_normalization.py job4_sink.py /opt/jobs/

USER flink
