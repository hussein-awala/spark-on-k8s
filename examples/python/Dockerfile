ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

COPY my_modules /opt/spark/work-dir/my_modules
COPY job.py /opt/spark/work-dir/
ENV PYTHONPATH="${PYTHONPATH}:/opt/spark/work-dir"
