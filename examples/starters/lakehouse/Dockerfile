FROM apache/spark:4.0.5-scala2.13-java17-python3-ubuntu
COPY examples/starters/target/iceberg-spark-runtime-4.0_2.13-1.12.0.jar /opt/spark/jars/
COPY examples/starters/lakehouse/job.py examples/starters/lakehouse/initial.json examples/starters/lakehouse/updates.json /opt/spark/work-dir/starters/lakehouse/
