from __future__ import annotations


def get_provider_info():
    return {
        "package-name": "spark-on-k8s",
        "name": "Spark On Kubernetes",
        "description": "An Airflow provider to use "
        "`spark-on-k8s <https://hussein.awala.fr/spark-on-k8s>`__ package.",
        "extra-links": ["spark_on_k8s.airflow.operator_links.SparkOnK8SOperatorLink"],
    }
