{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "benji.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a fully qualified resource name
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "benji.fullname" -}}
{{-   $envAll := index . 0 -}}
{{-   $resourceName := index . 1 -}}
{{-   if ne $resourceName "" -}}
{{-     if $envAll.Values.resourceNameOverride -}}
{{-       printf "%s-%s" ($envAll.Values.resourceNameOverride | trunc (int (sub 63 (len $resourceName)))) $resourceName | trunc 63 | trimSuffix "-" -}}
{{-     else -}}
{{-       printf "%s-%s" ($envAll.Release.Name | trunc (int (sub 63 (len $resourceName)))) $resourceName | trunc 63 | trimSuffix "-" -}}
{{-     end -}}
{{-   else -}}
{{-     if $envAll.Values.resourceNameOverride -}}
{{-       printf "%s" $envAll.Values.resourceNameOverride | trunc 63 | trimSuffix "-" -}}
{{-     else -}}
{{-       printf "%s" $envAll.Release.Name | trunc 63 | trimSuffix "-" -}}
{{-     end -}}
{{-   end -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "benji.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
