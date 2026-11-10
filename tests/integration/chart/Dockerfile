FROM python:3.11-slim@sha256:65bd3d9f1d7fa268dd1909973ba7eb4bdf41f6dcf2cfbe76ba9528b170f3fd79
# The same fixed identity as docker/Dockerfile, so chart checks run the image's own non-root user.
RUN groupadd --gid 10001 spark-on-k8s \
    && useradd --uid 10001 --gid 10001 --no-log-init --create-home --home-dir /home/spark-on-k8s \
        --shell /usr/sbin/nologin spark-on-k8s \
    && install -d -o 10001 -g 10001 -m 0770 /var/lib/spark-on-k8s
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt
COPY dist/ /tmp/dist/
RUN pip install --no-deps /tmp/dist/*.whl && pip check
USER 10001:10001
ENTRYPOINT ["spark-on-k8s", "api", "start", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
