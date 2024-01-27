#!/bin/bash

spark-on-k8s api start \
  --host "$SPARK_ON_K8S_API_HOST" \
  --port "$SPARK_ON_K8S_API_PORT" \
  --workers "$SPARK_ON_K8S_API_WORKERS" \
  --log-level "$SPARK_ON_K8S_API_LOG_LEVEL" \
  --limit-concurrency "$SPARK_ON_K8S_API_LIMIT_CONCURRENCY"