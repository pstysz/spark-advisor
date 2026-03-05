{{/*
NATS URL resolved from release name.
Subcharts use this when their config.nats.url is empty.
*/}}
{{- define "spark-advisor.natsUrl" -}}
nats://{{ .Release.Name }}-nats:4222
{{- end }}
