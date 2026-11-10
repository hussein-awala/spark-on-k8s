FROM python:3.11-slim@sha256:65bd3d9f1d7fa268dd1909973ba7eb4bdf41f6dcf2cfbe76ba9528b170f3fd79

ENV SPARK_ON_K8S_API_HOST=127.0.0.1
ENV SPARK_ON_K8S_API_PORT=8000
ENV SPARK_ON_K8S_API_WORKERS=4
ENV SPARK_ON_K8S_API_LOG_LEVEL=info
ENV SPARK_ON_K8S_API_LIMIT_CONCURRENCY=1000

# Fixed numeric identity: Kubernetes can only verify runAsNonRoot for a numeric USER, and the
# chart's runAsUser, runAsGroup and fsGroup defaults must match it. Keep them in sync.
RUN groupadd --gid 10001 spark-on-k8s \
    && useradd --uid 10001 --gid 10001 --no-log-init --create-home --home-dir /home/spark-on-k8s \
        --shell /usr/sbin/nologin spark-on-k8s \
    && install -d -o 10001 -g 10001 -m 0770 /var/lib/spark-on-k8s

COPY --chmod=755 scripts/entrypoint.sh /entrypoint.sh
# Prepared by scripts/release_artifacts.py from the exact release wheel and uv export.
COPY release/ /opt/spark-on-k8s/release/
RUN cd /opt/spark-on-k8s/release \
    && sha256sum --check inputs.sha256 \
    && python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.txt \
    && python -m pip install --no-cache-dir --no-deps --no-index ./*.whl \
    && python -m pip check \
    && python verify.py inventory --directory /opt/spark-on-k8s/release

# Packages stay root-owned and read-only to the API; run-history data goes to /var/lib/spark-on-k8s.
USER 10001:10001

ENTRYPOINT ["/entrypoint.sh"]
