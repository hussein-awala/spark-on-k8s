#!/usr/bin/env bash
# Opt-in end-to-end check of examples/sso-gateway; it is not run by CI workflows.
# Run from the repository root against a disposable kind cluster named sso-gateway-ci
# whose node can pull public images and already has API_IMAGE loaded (build it like
# .github/workflows/chart-kubernetes.yml). A mock OIDC provider replaces the issuer.
# The example is checked at the host root, then with the documented /platform/spark prefix.
set -euo pipefail
# Only run against the dedicated disposable cluster.
[[ "$(kubectl config current-context)" == kind-sso-gateway-ci ]]
API_IMAGE="${API_IMAGE:-ghcr.io/hussein-awala/spark-on-k8s:chart-ci}"
HERE=tests/integration/sso_gateway
EXAMPLE=examples/sso-gateway
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
diagnostics() {
  kubectl get pods,networkpolicies --all-namespaces -o wide || true
  kubectl logs --namespace spark-system deployment/spark-console-gateway --tail 100 || true
  kubectl logs --namespace spark-system deployment/spark-console-spark-on-k8s --tail 100 || true
}
trap diagnostics ERR

run_client() {
  local name="$1" prefix="$2" phase=""
  kubectl apply --namespace spark-system -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: $API_IMAGE
      imagePullPolicy: Never
      command: ["python", "/client/client.py"]
      env:
        - name: SSO_PREFIX
          value: "$prefix"
      volumeMounts:
        - name: client
          mountPath: /client
  volumes:
    - name: client
      configMap:
        name: sso-client
EOF
  for _ in $(seq 90); do
    phase="$(kubectl get --namespace spark-system "pod/$name" -o jsonpath='{.status.phase}')"
    [[ "$phase" == Succeeded || "$phase" == Failed ]] && break
    sleep 2
  done
  kubectl logs --namespace spark-system "pod/$name"
  [[ "$phase" == Succeeded ]]
}

kubectl create namespace sso-fixtures
kubectl create namespace spark-system
sed "s|API_IMAGE|$API_IMAGE|" "$HERE/fixtures.yaml" | kubectl apply --namespace sso-fixtures -f -
kubectl rollout status --namespace sso-fixtures deployment/mock-oidc --timeout 180s
kubectl rollout status --namespace sso-fixtures deployment/header-echo --timeout 120s

install_api() {
  helm upgrade --install spark-console chart --namespace spark-system -f "$1" \
    --set image.repository="${API_IMAGE%:*}" --set-string image.tag="${API_IMAGE##*:}" \
    --set image.pullPolicy=Never --set namespaces=sso-fixtures --wait --timeout 180s
}
install_api "$EXAMPLE/console-values.yaml"
# Kubelet probes must keep passing once only gateway pods may connect to the API.
kubectl apply --namespace spark-system -f "$EXAMPLE/network-policy.yaml"
kubectl rollout restart --namespace spark-system deployment/spark-console-spark-on-k8s
kubectl rollout status --namespace spark-system deployment/spark-console-spark-on-k8s --timeout 180s

# Disposable-cluster values generated here; the mock provider accepts any client secret.
kubectl create secret generic spark-console-gateway --namespace spark-system \
  --from-literal=client-secret=sso-ci-disposable-client-secret \
  --from-literal=cookie-secret="$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
sed "s|https://login.example.com|http://mock-oidc.sso-fixtures.svc.cluster.local:8080/default|" \
  "$EXAMPLE/gateway.yaml" > "$WORK/gateway.yaml"
kubectl apply --namespace spark-system -f "$WORK/gateway.yaml"
kubectl rollout status --namespace spark-system deployment/spark-console-gateway --timeout 180s

kubectl create configmap sso-client --namespace spark-system --from-file=client.py="$HERE/client.py"
run_client sso-client-root ""

# Apply the path-prefix changes documented in docs/user-guide/sso-gateway.md.
python3 - "$EXAMPLE/console-values.yaml" "$WORK/gateway.yaml" "$WORK" <<'PY'
import sys
from pathlib import Path


def edit(source, target, replacements):
    text = Path(source).read_text()
    for old, new in replacements:
        assert text.count(old) == 1, f"{source}: {old!r} must occur exactly once"
        text = text.replace(old, new)
    Path(target).write_text(text)


prefix = "/platform/spark"
edit(sys.argv[1], f"{sys.argv[3]}/console-values-prefix.yaml", [(
    "    value: https://spark.example.com\n",
    f"    value: https://spark.example.com\n  - name: SPARK_ON_K8S_API_ROOT_PATH\n    value: {prefix}\n",
)])
edit(sys.argv[2], sys.argv[2], [
    ("          path: /\n", f"          path: ^{prefix}/(.*)$\n          rewriteTarget: /$1\n"),
    ("/oauth2/callback\n", f"{prefix}/oauth2/callback\n            - --proxy-prefix={prefix}/oauth2\n"),
    ("--api-route=^/(", f"--api-route=^{prefix}/("),
    ("--ping-path=/health", f"--ping-path={prefix}/health"),
    ("--ready-path=/ready", f"--ready-path={prefix}/ready"),
    ("--cookie-name=__Host-spark-console", f"--cookie-name=__Secure-spark-console\n            - --cookie-path={prefix}"),
    ("              path: /health\n", f"              path: {prefix}/health\n"),
    ("              path: /ready\n", f"              path: {prefix}/ready\n"),
    ("          - path: /\n", f"          - path: {prefix}\n"),
])
PY
install_api "$WORK/console-values-prefix.yaml"
kubectl apply --namespace spark-system -f "$WORK/gateway.yaml"
kubectl rollout status --namespace spark-system deployment/spark-console-gateway --timeout 180s
run_client sso-client-prefix /platform/spark
