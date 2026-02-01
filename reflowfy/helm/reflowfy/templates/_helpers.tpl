{{/*
Expand the name of the chart.
*/}}
{{- define "reflowfy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "reflowfy.fullname" -}}
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
{{- define "reflowfy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "reflowfy.labels" -}}
helm.sh/chart: {{ include "reflowfy.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: reflowfy
{{- end }}

{{/*
PostgreSQL host - returns internal service name if deployed, external host otherwise
*/}}
{{- define "reflowfy.postgresql.host" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql" .Release.Name }}
{{- else }}
{{- .Values.postgresql.external.host }}
{{- end }}
{{- end }}

{{/*
PostgreSQL port
*/}}
{{- define "reflowfy.postgresql.port" -}}
{{- if .Values.postgresql.enabled }}
{{- 5432 }}
{{- else }}
{{- .Values.postgresql.external.port | default 5432 }}
{{- end }}
{{- end }}

{{/*
PostgreSQL database
*/}}
{{- define "reflowfy.postgresql.database" -}}
{{- if .Values.postgresql.enabled }}
{{- .Values.postgresql.auth.database }}
{{- else }}
{{- .Values.postgresql.external.database | default "reflowfy" }}
{{- end }}
{{- end }}

{{/*
PostgreSQL username
*/}}
{{- define "reflowfy.postgresql.username" -}}
{{- if .Values.postgresql.enabled }}
{{- .Values.postgresql.auth.username }}
{{- else }}
{{- .Values.postgresql.external.username | default "reflowfy" }}
{{- end }}
{{- end }}

{{/*
PostgreSQL password secret name
*/}}
{{- define "reflowfy.postgresql.secretName" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql" .Release.Name }}
{{- else if .Values.postgresql.external.existingSecret }}
{{- .Values.postgresql.external.existingSecret }}
{{- else }}
{{- printf "%s-postgresql-external" .Release.Name }}
{{- end }}
{{- end }}

{{/*
PostgreSQL password secret key
*/}}
{{- define "reflowfy.postgresql.secretKey" -}}
{{- if .Values.postgresql.enabled -}}
password
{{- else if .Values.postgresql.external.existingSecret -}}
{{- .Values.postgresql.external.existingSecretPasswordKey | default "password" -}}
{{- else -}}
password
{{- end -}}
{{- end }}

{{/*
Database URL (with password placeholder for envsubst or direct injection)
*/}}
{{- define "reflowfy.databaseUrl" -}}
postgresql://{{ include "reflowfy.postgresql.username" . }}:$(DATABASE_PASSWORD)@{{ include "reflowfy.postgresql.host" . }}:{{ include "reflowfy.postgresql.port" . }}/{{ include "reflowfy.postgresql.database" . }}
{{- end }}

{{/*
Kafka bootstrap servers - returns internal service name if deployed, external servers otherwise
*/}}
{{- define "reflowfy.kafka.bootstrapServers" -}}
{{- if .Values.kafka.enabled }}
{{- printf "%s-kafka:9092" .Release.Name }}
{{- else }}
{{- .Values.kafka.external.bootstrapServers }}
{{- end }}
{{- end }}

{{/*
Kafka topic
*/}}
{{- define "reflowfy.kafka.topic" -}}
{{- .Values.kafka.topic | default "reflow.jobs" }}
{{- end }}

{{/*
Kafka group ID
*/}}
{{- define "reflowfy.kafka.groupId" -}}
{{- .Values.kafka.groupId | default "reflowfy-workers" }}
{{- end }}
