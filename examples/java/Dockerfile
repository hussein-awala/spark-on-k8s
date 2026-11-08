ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM maven:3.9-eclipse-temurin-17 AS builder

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY_VERSION=2.12
COPY job /job
WORKDIR /job
RUN mvn --batch-mode -Dspark.version="${SPARK_VERSION}" \
    -Dscala.binary.version="${SCALA_BINARY_VERSION}" clean package

FROM ${SPARK_IMAGE}
COPY --from=builder /job/target/java-job-1.0-SNAPSHOT.jar /java-job.jar
