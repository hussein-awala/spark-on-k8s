FROM maven:3.9-amazoncorretto-11 AS builder

ADD job /job

WORKDIR /job

RUN mvn clean package

FROM husseinawala/spark:v3.5.0

COPY --from=builder /job/target/java-job-1.0-SNAPSHOT.jar /java-job.jar
