{{- define "reflowfy-api.fullname" -}}
{{- .Release.Name }}-{{ .Chart.Name }}
{{- end }}
