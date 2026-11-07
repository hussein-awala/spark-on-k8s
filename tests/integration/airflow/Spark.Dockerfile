ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}
COPY spark_job.py /opt/spark/work-dir/airflow_job.py
