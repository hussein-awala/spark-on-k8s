FROM python:3.11-slim@sha256:65bd3d9f1d7fa268dd1909973ba7eb4bdf41f6dcf2cfbe76ba9528b170f3fd79

ENV SPARK_ON_K8S_API_HOST=127.0.0.1
ENV SPARK_ON_K8S_API_PORT=8000
ENV SPARK_ON_K8S_API_WORKERS=4
ENV SPARK_ON_K8S_API_LOG_LEVEL=info
ENV SPARK_ON_K8S_API_LIMIT_CONCURRENCY=1000

COPY --chmod=755 scripts/entrypoint.sh /entrypoint.sh
# Prepared by scripts/release_artifacts.py from the exact release wheel and uv export.
COPY release/ /opt/spark-on-k8s/release/
RUN cd /opt/spark-on-k8s/release \
    && sha256sum --check inputs.sha256 \
    && python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.txt \
    && python -m pip install --no-cache-dir --no-deps --no-index ./*.whl \
    && python -m pip check \
    && python verify.py inventory --directory /opt/spark-on-k8s/release

ENTRYPOINT ["/entrypoint.sh"]
