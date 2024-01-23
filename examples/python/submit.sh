spark-on-k8s app submit \
  --image pyspark-job \
  --path local:///opt/spark/work-dir/job.py \
  --namespace spark \
  --name pyspark-job-example \
  --service-account spark \
  --image-pull-policy Never \
  --driver-cpu 1 \
  --driver-memory 512 \
  --driver-memory-overhead 128 \
  --executor-cpu 1 \
  --executor-memory 512 \
  --executor-memory-overhead 128 \
  --executor-initial-instances 5 \
  --ui-reverse-proxy \
  --logs \
  100000
