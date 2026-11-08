FROM python:3.11-slim@sha256:65bd3d9f1d7fa268dd1909973ba7eb4bdf41f6dcf2cfbe76ba9528b170f3fd79
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt
COPY dist/ /tmp/dist/
RUN pip install --no-deps /tmp/dist/*.whl && pip check
ENTRYPOINT ["spark-on-k8s", "api", "start", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
