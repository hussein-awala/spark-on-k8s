FROM --platform=linux/amd64 apache/hadoop:3.5.0@sha256:46980ea1653eb3453d9cf43c4b7cb69c4729e19612d4902da0372d11e11a7a12 AS distribution
FROM apache/spark:4.3.0-scala2.13-java21-python3-ubuntu@sha256:3dedc5c8665a6f472e2f0a3a828207a79a30785090ef325d3af219cb90a73d63
COPY --from=distribution --chown=185:0 /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop HADOOP_LOG_DIR=/tmp/hadoop-logs HADOOP_HEAPSIZE_MAX=256
ENV PATH=/opt/hadoop/bin:${PATH}
USER 185
ENTRYPOINT ["/bin/bash"]
