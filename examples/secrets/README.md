## Submit a Spark application with secrets

This example shows how to submit a Spark application with secrets.
```bash
docker build -t pyspark-job:with-secrets .
```
```bash
# For minikube
minikube image load pyspark-job:with-secrets
```
```bash
# For kind
kind load docker-image pyspark-job:with-secrets
```
```bash
# For remote clusters, you will need to change the image name to match your registry,
# and then push it to that registry
docker push pyspark-job:with-secrets
```
Then, submit the job:
```bash
python submit.py
```
Or via the bash script:
```bash
./submit.sh
```
