FROM python:3.14-slim@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f
# The pinned base can lag Debian security updates (for example zlib1g); install them so the
# released image carries every fix published for its base.
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
# The same fixed identity as docker/Dockerfile, so chart checks run the image's own non-root user.
RUN groupadd --gid 10001 spark-on-k8s \
    && useradd --uid 10001 --gid 10001 --no-log-init --create-home --home-dir /home/spark-on-k8s \
        --shell /usr/sbin/nologin spark-on-k8s \
    && install -d -o 10001 -g 10001 -m 0770 /var/lib/spark-on-k8s
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt
COPY dist/ /tmp/dist/
# Like docker/Dockerfile, the image keeps no packaging tools after installing.
RUN pip install --no-deps /tmp/dist/*.whl && pip check \
    && python -m pip uninstall --yes setuptools wheel pip
USER 10001:10001
ENTRYPOINT ["spark-on-k8s", "api", "start", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
