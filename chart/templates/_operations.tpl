{{/*
PodDisruptionBudget limit. Arguments: budget (the values map), replicas (the smallest
replica count) and path (the values path, for messages).
*/}}
{{- define "spark-on-k8s.disruptionBudget" -}}
{{- $budget := .budget -}}
{{- $hasMin := not (kindIs "invalid" $budget.minAvailable) -}}
{{- $hasMax := not (kindIs "invalid" $budget.maxUnavailable) -}}
{{- if and $hasMin $hasMax -}}
{{- fail (printf "%s sets both minAvailable and maxUnavailable; set only one" .path) -}}
{{- end -}}
{{- $field := "maxUnavailable" -}}
{{- $value := 1 -}}
{{- if $hasMin -}}
{{- $field = "minAvailable" -}}
{{- $value = $budget.minAvailable -}}
{{- else if $hasMax -}}
{{- $value = $budget.maxUnavailable -}}
{{- end -}}
{{- $replicas := int .replicas -}}
{{- if gt $replicas 0 -}}
{{- /* Kubernetes rounds both percentages up when scaling them to the replica count. */ -}}
{{- $scaled := 0 -}}
{{- if kindIs "string" $value -}}
{{- $scaled = div (add (mul (int (trimSuffix "%" $value)) $replicas) 99) 100 -}}
{{- else -}}
{{- $scaled = int $value -}}
{{- end -}}
{{- $allowed := $scaled -}}
{{- if $hasMin -}}{{- $allowed = sub $replicas $scaled -}}{{- end -}}
{{- if le (int $allowed) 0 -}}
{{- fail (printf "%s.%s %v permits no voluntary disruption with %d replica(s), which blocks node drains; allow at least one unavailable pod or add replicas" .path $field $value $replicas) -}}
{{- end -}}
{{- end -}}
{{ $field }}: {{ if kindIs "string" $value }}{{ $value | quote }}{{ else }}{{ int $value }}{{ end }}
{{- end }}

{{/*
Namespace selector for the namespaces the API serves, as resolved by spark-on-k8s.namespaces.
*/}}
{{- define "spark-on-k8s.namespaceSelector" -}}
{{- $scope := include "spark-on-k8s.namespaces" . -}}
{{- if eq $scope "*" -}}
namespaceSelector: {}
{{- else -}}
namespaceSelector:
  matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: In
      values:
        {{- range splitList "," $scope }}
        - {{ . | quote }}
        {{- end }}
{{- end -}}
{{- end }}
