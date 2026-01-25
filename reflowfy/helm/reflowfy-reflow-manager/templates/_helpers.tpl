{{/*
Expand the name of the chart.
*/}}
{{- define "reflowfy-reflow-manager.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "reflowfy-reflow-manager.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "reflowfy-reflow-manager.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "reflowfy-reflow-manager.labels" -}}
helm.sh/chart: {{ include "reflowfy-reflow-manager.chart" . }}
{{ include "reflowfy-reflow-manager.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "reflowfy-reflow-manager.selectorLabels" -}}
app.kubernetes.io/name: {{ include "reflowfy-reflow-manager.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: reflowfy-reflow-manager
{{- end }}

{{/*
Database URL helper
*/}}
{{- define "reflowfy-reflow-manager.databaseUrl" -}}
{{- if .Values.postgresql.existingSecret }}
{{- /* Password from secret, URL constructed at runtime */ -}}
postgresql://{{ .Values.postgresql.username }}:$(DATABASE_PASSWORD)@{{ .Values.postgresql.host }}:{{ .Values.postgresql.port }}/{{ .Values.postgresql.database }}
{{- else }}
postgresql://{{ .Values.postgresql.username }}:{{ .Values.postgresql.password }}@{{ .Values.postgresql.host }}:{{ .Values.postgresql.port }}/{{ .Values.postgresql.database }}
{{- end }}
{{- end }}
