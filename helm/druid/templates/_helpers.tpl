{{/*

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

*/}}

{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "druid.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "druid.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "druid.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified historical name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.historical.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.historical.name }}
{{- end -}}

{{/*
Create a default fully qualified middleManager name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.middleManager.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.middleManager.name }}
{{- end -}}


{{/*
Create a default fully qualified broker name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.broker.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.broker.name }}
{{- end -}}

{{/*
Create a default fully qualified overlord name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.overlord.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.overlord.name }}
{{- end -}}

{{/*
Create a default fully qualified coordinator name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.coordinator.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.coordinator.name }}
{{- end -}}

{{/*
Create a default fully qualified router name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "druid.router.fullname" -}}
{{ template "druid.fullname" . }}-{{ .Values.router.name }}
{{- end -}}

{{/*
Create the name of the broker service account
*/}}
{{- define "druid.broker.serviceAccountName" -}}
  {{- if .Values.broker.serviceAccount.create }}
    {{- default (include "druid.broker.fullname" .) .Values.broker.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.broker.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Create the name of the historical service account
*/}}
{{- define "druid.historical.serviceAccountName" -}}
  {{- if .Values.historical.serviceAccount.create }}
    {{- default (include "druid.historical.fullname" .) .Values.historical.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.historical.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Create the name of the middleManager service account
*/}}
{{- define "druid.middleManager.serviceAccountName" -}}
  {{- if .Values.middleManager.serviceAccount.create }}
    {{- default (include "druid.middleManager.fullname" .) .Values.middleManager.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.middleManager.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Create the name of the coordinator service account
*/}}
{{- define "druid.coordinator.serviceAccountName" -}}
  {{- if .Values.coordinator.serviceAccount.create }}
    {{- default (include "druid.coordinator.fullname" .) .Values.coordinator.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.coordinator.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Create the name of the overlord service account
*/}}
{{- define "druid.overlord.serviceAccountName" -}}
  {{- if .Values.overlord.serviceAccount.create }}
    {{- default (include "druid.overlord.fullname" .) .Values.overlord.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.overlord.serviceAccount.name }}
  {{- end }}
{{- end }}

{{/*
Create the name of the router service account
*/}}
{{- define "druid.router.serviceAccountName" -}}
  {{- if .Values.router.serviceAccount.create }}
    {{- default (include "druid.router.fullname" .) .Values.router.serviceAccount.name }}
  {{- else }}
    {{- default "default" .Values.router.serviceAccount.name }}
  {{- end }}
{{- end }}
