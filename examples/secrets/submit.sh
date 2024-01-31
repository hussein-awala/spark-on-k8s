spark-on-k8s app submit \
  --image pyspark-job:with-secrets \
  --path local:///opt/spark/work-dir/job.py \
  --namespace spark \
  --name spark-application-with-secrets-example \
  --service-account spark \
  --image-pull-policy Never \
  --driver-cpu 1 \
  --driver-memory 512 \
  --driver-memory-overhead 128 \
  --executor-cpu 1 \
  --executor-memory 512 \
  --executor-memory-overhead 128 \
  --executor-initial-instances 5 \
  --secret-env-var NUM_POINTS_SECRET=100000 \
  --secret-env-var ANOTHER_SECRET="This is a secret" \
  --ui-reverse-proxy \
  --logs

