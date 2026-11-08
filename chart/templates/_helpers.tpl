{{/*
Expand the name of the chart.
*/}}
{{- define "spark-on-k8s.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spark-on-k8s.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "spark-on-k8s.sparkHistory.fullname" -}}
{{- include "spark-on-k8s.historyName" (include "spark-on-k8s.fullname" .) }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "spark-on-k8s.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "spark-on-k8s.labels" -}}
helm.sh/chart: {{ include "spark-on-k8s.chart" . }}
{{ include "spark-on-k8s.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spark-on-k8s.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spark-on-k8s.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Spark history Selector labels
*/}}
{{- define "spark-on-k8s.sparkHistory.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spark-on-k8s.historyName" (include "spark-on-k8s.name" .) }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "spark-on-k8s.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "spark-on-k8s.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the service account to use for the history server
*/}}
{{- define "spark-on-k8s.sparkHistory.serviceAccountName" -}}
{{- if .Values.sparkHistory.serviceAccount.create }}
{{- default (include "spark-on-k8s.sparkHistory.fullname" .) .Values.sparkHistory.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.sparkHistory.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Preserve the legacy ownership default when rbac.create is null/absent. */}}
{{- define "spark-on-k8s.rbacCreate" -}}
{{- if kindIs "bool" .Values.rbac.create -}}
{{- .Values.rbac.create -}}
{{- else -}}
{{- .Values.serviceAccount.create -}}
{{- end -}}
{{- end }}

{{/* Digest selection is shared by the API and independently versioned History Server. */}}
{{- define "spark-on-k8s.image" -}}
{{- if .image.digest -}}
{{- printf "%s@%s" .image.repository .image.digest -}}
{{- else -}}
{{- $tag := default .defaultTag .image.tag -}}
{{- if eq $tag "unreleased" -}}{{- fail "Source chart requires an explicit qualified image.tag or image.digest" -}}{{- end -}}
{{- printf "%s:%s" .image.repository (required .error $tag) -}}
{{- end -}}
{{- end }}

{{/* Preserve normal existing names; disambiguate the suffix collision. */}}
{{- define "spark-on-k8s.historyName" -}}
{{- $candidate := printf "%s-history" (. | trunc 55 | trimSuffix "-") -}}
{{- if eq $candidate . -}}
{{- printf "%s-%s-history" (. | trunc 46 | trimSuffix "-") (. | sha256sum | trunc 8) -}}
{{- else -}}{{ $candidate }}{{- end -}}
{{- end -}}

{{/* Shared RBAC objects must identify both release name and namespace. */}}
{{- define "spark-on-k8s.rbacName" -}}
{{- $name := include "spark-on-k8s.fullname" . -}}
{{- if .Values.rbac.legacyNames -}}{{ $name }}
{{- else -}}
{{- printf "%s-%s" ($name | trunc 49 | trimSuffix "-") (printf "%s/%s" .Release.Namespace $name | sha256sum | trunc 8) -}}
{{- end -}}
{{- end -}}
