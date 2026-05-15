# ── HPS Flink custom image ────────────────────────────────────────────────────
# Base : apache/flink:1.19.1-scala_2.12-java11
# Adds : Python 3, PyFlink 1.19, Kafka connector JAR, pipeline job files
#
# Build (inside Minikube's Docker daemon):
#   eval $(minikube docker-env)
#   docker build -t hps-flink:1.0 ~/hps-rt-poc/flink-jobs/
# ─────────────────────────────────────────────────────────────────────────────
FROM apache/flink:1.19.1-scala_2.12-java11

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        wget \
        ca-certificates \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
# apache-flink must match the Flink runtime version exactly.
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ── Flink Kafka connector JARs ────────────────────────────────────────────────
# flink-connector-kafka-3.2.0-1.19 is the connector for Flink 1.19, but it is
# a thin JAR — it does NOT bundle kafka-clients.  Without kafka-clients on the
# classpath, Flink raises NoClassDefFoundError for OffsetResetStrategy at
# job submission time.  Both JARs must be in /opt/flink/lib/ together.
#
# Versions pinned:
#   flink-connector-kafka 3.2.0-1.19  (matches Flink 1.19.x)
#   kafka-clients         3.4.0        (the version the connector was built against)
RUN wget -q -P /opt/flink/lib/ \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar" \
 && wget -q -P /opt/flink/lib/ \
    "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar"

# ── Job files ─────────────────────────────────────────────────────────────────
RUN mkdir -p /opt/jobs
COPY . /opt/jobs/

# ── Python path ───────────────────────────────────────────────────────────────
# Makes `from common.config import ...` work without sys.path manipulation
# in each job file.
ENV PYTHONPATH=/opt/jobs

# ── Flink Python path ─────────────────────────────────────────────────────────
ENV PYFLINK_PYTHON=/usr/bin/python3
