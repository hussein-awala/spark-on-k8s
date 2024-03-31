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
{{- printf "%s-%s" (include "spark-on-k8s.fullname" .) "history" | trunc 63 | trimSuffix "-" }}
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
app.kubernetes.io/name: {{ include "spark-on-k8s.name" . }}-history
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
