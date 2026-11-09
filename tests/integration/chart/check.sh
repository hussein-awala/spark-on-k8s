#!/usr/bin/env bash
set -euo pipefail
# Only run against the dedicated disposable CI cluster.
[[ "$(kubectl config current-context)" == kind-chart-ci ]]
kubectl create namespace analytics
for namespace in control-a control-b; do
  helm install console chart --namespace "$namespace" --create-namespace \
    --set image.tag=chart-ci --set image.pullPolicy=Never \
    --set namespaces=analytics --set readOnly=false --wait --timeout 180s
  helm test console --namespace "$namespace" --timeout 120s
  test "$(kubectl auth can-i get pods/exec --namespace analytics \
    --as="system:serviceaccount:$namespace:console-spark-on-k8s")" = yes
done
# Upgrade the same release and verify health through its actual Service.
helm upgrade console chart --namespace control-a --reuse-values \
  --set metrics.enabled=true --wait --timeout 180s
helm test console --namespace control-a --timeout 120s
# Service DNS is not bounded by RBAC; the API scope must refuse other namespaces.
kubectl exec --namespace control-a deployment/console-spark-on-k8s -- python -c '
import json, urllib.request, urllib.error
for path in ("/webserver/ui/kube-system/kube-dns/", "/webserver/api/apps?namespace=kube-system"):
    try:
        urllib.request.urlopen("http://127.0.0.1:8000" + path)
    except urllib.error.HTTPError as error:
        assert error.code == 403, (path, error.code)
        assert json.load(error)["error"]["code"] == "namespace_forbidden", path
    else:
        raise AssertionError(path + " reached a namespace outside the API scope")
with urllib.request.urlopen("http://127.0.0.1:8000/webserver/api/apps?namespace=analytics") as response:
    assert response.status == 200
'
helm uninstall console --namespace control-a
# Removing one release must not remove the second release's permissions.
test "$(kubectl auth can-i get pods/exec --namespace analytics \
  --as=system:serviceaccount:control-b:console-spark-on-k8s)" = yes
helm test console --namespace control-b --timeout 120s
# Fixed public fixture token, confined to this disposable cluster.
kubectl create secret generic remote-test --namespace control-b \
  --from-literal=token=chart-ci-public-test-token-at-least-32-characters \
  --from-file=policy.json=tests/integration/chart/policy.json
helm upgrade console chart --namespace control-b --reuse-values \
  -f tests/integration/chart/remote-values.yaml --wait --timeout 180s
helm test console --namespace control-b --timeout 120s
kubectl exec --namespace control-b deployment/console-spark-on-k8s -- python -c '
import urllib.request, urllib.error
try:
    urllib.request.urlopen("http://127.0.0.1:8000/")
except urllib.error.HTTPError as error:
    assert error.code == 401, error.code
else:
    raise AssertionError("Root endpoint must require authentication")
'
