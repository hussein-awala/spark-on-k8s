## Submit a Spark application with secrets

This example shows how to submit a Spark application with secrets. From the repository root,
change into the example directory before running the commands below:

```bash
cd examples/secrets
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
# For a remote cluster, replace the example registry and push the tagged image. Then use
# that image name and the IfNotPresent pull policy in submit.py and submit.sh.
docker tag pyspark-job:with-secrets registry.example.com/team/pyspark-job:with-secrets
docker push registry.example.com/team/pyspark-job:with-secrets
```
Then, submit the job:
```bash
python submit.py
```
Or via the bash script:
```bash
./submit.sh
```
