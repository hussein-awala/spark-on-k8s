ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}
COPY tests/integration/spark/runtime_job.py /opt/spark/work-dir/runtime_job.py
COPY tests/integration/spark/target/integration.jar /opt/spark/work-dir/integration.jar
COPY examples/starters/etl/job.py examples/starters/etl/orders.json /opt/spark/work-dir/starters/etl/
COPY examples/starters/lakehouse/job.py examples/starters/lakehouse/initial.json examples/starters/lakehouse/updates.json /opt/spark/work-dir/starters/lakehouse/
COPY tests/integration/starters/check.py /opt/spark/work-dir/check_starters.py
