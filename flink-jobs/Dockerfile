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

# ── Flink Kafka connector JAR ─────────────────────────────────────────────────
# flink-connector-kafka 3.2.0-1.19 is the stable release for Flink 1.19.
# Placed in /opt/flink/lib/ so it is auto-loaded by the Flink class loader
# without any env.add_jars() call in the Python job code.
RUN wget -q -P /opt/flink/lib/ \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar"

# ── Job files ─────────────────────────────────────────────────────────────────
RUN mkdir -p /opt/jobs
COPY . /opt/jobs/

# ── Python path ───────────────────────────────────────────────────────────────
# Makes `from common.config import ...` work without sys.path manipulation
# in each job file.
ENV PYTHONPATH=/opt/jobs

# ── Flink Python path ─────────────────────────────────────────────────────────
ENV PYFLINK_PYTHON=/usr/bin/python3
