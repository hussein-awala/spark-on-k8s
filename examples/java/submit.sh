spark-on-k8s app submit \
  --image java-spark-job \
  --path local:///java-job.jar \
  --namespace spark \
  --name spark-java-job-example \
  --service-account spark \
  --image-pull-policy Never \
  --executor-min-instances 0 \
  --executor-max-instances 5 \
  --executor-initial-instances 5 \
  --ui-reverse-proxy \
  --logs \
  --class com.oss_tech.examples.TestJob \
  100000
