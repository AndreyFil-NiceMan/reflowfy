{{- define "reflowfy-worker.fullname" -}}
{{- .Release.Name }}-{{ .Chart.Name }}
{{- end }}
