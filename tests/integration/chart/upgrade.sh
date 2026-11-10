#!/usr/bin/env bash
# Upgrade releases of the newest published chart to the source chart, then roll back.
# Run from the repository root after loading the chart-ci API image into the cluster.
set -euo pipefail
# Only run against the dedicated disposable CI cluster.
[[ "$(kubectl config current-context)" == kind-chart-upgrade-ci ]]

# The baseline and the API image of its appVersion, both pinned by digest. Move it to
# each newly published chart (docs/releases.md).
published_repository=https://hussein.awala.fr/spark-on-k8s-chart
published_chart=0.3.0
published_chart_sha256=fe7ef05a8e69bf083cc1e21c559850f4351fca85ba9b4dd34c3d34979b478ced
published_image_tag=0.6.0@sha256:38ff448d66824bc287cde677354518e0658f04fc702df03b29c82d8d0d5cf259
repository=ghcr.io/hussein-awala/spark-on-k8s
source_chart=$(sed -n 's/^version: //p' chart/Chart.yaml)
# The source chart runs the API as non-root UID 10001; the published chart sets no user,
# so its root API image must run as root again after a rollback.
source_user=10001
account=console-spark-on-k8s
deployment=deployment/console-spark-on-k8s

fail() {
  echo "$*" >&2
  exit 1
}

expect_release() { # namespace revision chart-version
  local metadata
  metadata=$(helm get metadata console --namespace "$1")
  if ! grep -qx "REVISION: $2" <<<"$metadata" || ! grep -qx "VERSION: $3" <<<"$metadata" \
    || ! grep -qx "STATUS: deployed" <<<"$metadata"; then
    fail "Expected revision $2 of chart $3 deployed in $1, found:"$'\n'"$metadata"
  fi
}

expect_api() { # namespace image readiness-path pod-user (empty: the image's own user)
  local container='{.spec.template.spec.containers[0]' user
  test "$(kubectl get "$deployment" --namespace "$1" -o jsonpath="$container.image}")" = "$2" \
    || fail "$1 does not run $2"
  test "$(kubectl get "$deployment" --namespace "$1" \
    -o jsonpath="$container.readinessProbe.httpGet.path}")" = "$3" || fail "$1 does not probe $3"
  user=$(kubectl get "$deployment" --namespace "$1" \
    -o jsonpath='{.spec.template.spec.securityContext.runAsUser}')
  test "$user" = "$4" || fail "$1 pods run as user '$user' instead of '$4'"
  kubectl rollout status "$deployment" --namespace "$1" --timeout 120s
  helm test console --namespace "$1" --timeout 120s
}

can() { # verb resource namespace release-namespace
  test "$(kubectl auth can-i "$1" "$2" --namespace "$3" \
    --as="system:serviceaccount:$4:$account")" = yes || fail "$4/$account cannot $1 $2 in $3"
}

uid() { # resource namespace
  kubectl get "$1" --namespace "$2" -o jsonpath='{.metadata.uid}'
}

same_uid() { # resource namespace expected-uid
  test "$(uid "$1" "$2")" = "$3" || fail "$2/$1 was replaced instead of updated in place"
}

absent() { # cluster-scoped resource
  test -z "$(kubectl get "$1" --ignore-not-found -o name)" || fail "$1 still exists"
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
helm pull spark-on-k8s --repo "$published_repository" --version "$published_chart" \
  --destination "$work"
published="$work/spark-on-k8s-$published_chart.tgz"
echo "$published_chart_sha256  $published" | sha256sum -c -

kubectl create namespace analytics
# upgrade-legacy edits one namespace and keeps its RBAC names. upgrade-default keeps the
# published defaults (every namespace, read-only) and moves to namespace-qualified names.
legacy=(--namespace upgrade-legacy -f tests/integration/chart/published-values.yaml
  --set namespaces=analytics --set readOnly=false)
default=(--namespace upgrade-default -f tests/integration/chart/published-values.yaml)
install=(--set-string image.tag="$published_image_tag" --create-namespace --wait --timeout 300s)
helm install console "$published" "${legacy[@]}" "${install[@]}"
helm install console "$published" "${default[@]}" "${install[@]}"
for namespace in upgrade-legacy upgrade-default; do
  expect_release "$namespace" 1 "$published_chart"
  expect_api "$namespace" "$repository:$published_image_tag" / ""
done
can create pods/exec analytics upgrade-legacy
can list pods analytics upgrade-default
role_uid=$(uid role/"$account" analytics)
binding_uid=$(uid rolebinding/"$account"-edit analytics)
kubectl get clusterrolebinding "$account"-view

# Plain --reuse-values keeps only the old values, so the schema rejects the new chart.
if output=$(helm upgrade console chart --namespace upgrade-default --reuse-values \
  --set image.tag=chart-ci --set image.pullPolicy=Never 2>&1); then
  fail "--reuse-values from chart $published_chart unexpectedly succeeded"
fi
grep -q "missing properties" <<<"$output" || fail "Unexpected --reuse-values failure: $output"
echo "Plain --reuse-values was refused: $output"
expect_release upgrade-default 1 "$published_chart"

# Documented path 1: pass the same values again, keeping the legacy RBAC names.
helm upgrade console chart "${legacy[@]}" --set image.tag=chart-ci --set image.pullPolicy=Never \
  --set rbac.legacyNames=true --wait --timeout 180s
expect_release upgrade-legacy 2 "$source_chart"
expect_api upgrade-legacy "$repository:chart-ci" /ready "$source_user"
# Same names: Helm updates the existing objects in place instead of replacing them.
same_uid role/"$account" analytics "$role_uid"
same_uid rolebinding/"$account"-edit analytics "$binding_uid"
can get pods/exec analytics upgrade-legacy

# Documented path 2: --reset-then-reuse-values with the default namespace-qualified names.
# The release relied on the old "*" default, so the upgrade sets it explicitly.
helm upgrade console chart --namespace upgrade-default --reset-then-reuse-values \
  --set image.tag=chart-ci --set image.pullPolicy=Never --set-string 'namespaces=*' \
  --wait --timeout 180s
expect_release upgrade-default 2 "$source_chart"
expect_api upgrade-default "$repository:chart-ci" /ready "$source_user"
suffix=$(printf '%s' "upgrade-default/$account" | sha256sum | cut -c1-8)
# "*" now binds a chart-owned ClusterRole of that name instead of the built-in view role.
kubectl get clusterrole "$account-$suffix"
kubectl get clusterrolebinding "$account-$suffix"
absent clusterrolebinding/"$account"-view
can list pods analytics upgrade-default

# Rolling back restores the published chart, its image and probes, and its RBAC names.
for namespace in upgrade-legacy upgrade-default; do
  helm rollback console 1 --namespace "$namespace" --wait --timeout 180s
  expect_release "$namespace" 3 "$published_chart"
  expect_api "$namespace" "$repository:$published_image_tag" / ""
done
same_uid role/"$account" analytics "$role_uid"
same_uid rolebinding/"$account"-edit analytics "$binding_uid"
can create pods/exec analytics upgrade-legacy
kubectl get clusterrolebinding "$account"-view
absent clusterrolebinding/"$account-$suffix"
absent clusterrole/"$account-$suffix"
can list pods analytics upgrade-default

for namespace in upgrade-legacy upgrade-default; do
  helm uninstall console --namespace "$namespace" --wait --timeout 120s
done
# The cluster-wide grant must not outlive its release.
absent clusterrolebinding/"$account"-view
kubectl delete namespace upgrade-legacy upgrade-default analytics --wait --timeout 120s
