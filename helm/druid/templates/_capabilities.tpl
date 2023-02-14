{{/*
Return the target Kubernetes version
*/}}
{{- define "capabilities.kubeVersion" -}}
{{- default .Capabilities.KubeVersion.Version .Values.kubeVersion -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for podDisruptionBudget.
*/}}
{{- define "capabilities.policy.apiVersion" -}}
  {{- if semverCompare "<1.21-0" (include "capabilities.kubeVersion" .) -}}
    {{- print "policy/v1beta1" -}}
  {{- else -}}
    {{- print "policy/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion and manifest format for ingress
*/}}
{{- define "capabilities.ingress.apiVersion" -}}
  {{- if semverCompare "<1.14-0" (include "capabilities.kubeVersion" .) -}}
    {{- print "extensions/v1beta1" -}}
  {{- else if semverCompare "<1.19-0" (include "capabilities.kubeVersion" .) -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "networking.k8s.io/v1" -}}
  {{- end }}
{{- end -}}

{{/* Check Ingress stability */}}
{{- define "capabilities.ingress.isStable" -}}
  {{- eq (include "capabilities.ingress.apiVersion" .) "networking.k8s.io/v1" -}}
{{- end -}}

{{/* Check Ingress supports pathType */}}
{{/* pathType was added to networking.k8s.io/v1beta1 in Kubernetes 1.18 */}}
{{- define "capabilities.ingress.supportsPathType" -}}
  {{- or (eq (include "capabilities.ingress.isStable" .) "true") (and (eq (include "capabilities.ingress.apiVersion" .) "networking.k8s.io/v1beta1") (semverCompare ">= 1.18-0" (include "capabilities.kubeVersion" .))) -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for Horizontal Pod Autoscaler.
*/}}
{{- define "capabilities.hpa.apiVersion" -}}
  {{- if semverCompare "<1.23-0" (include "capabilities.kubeVersion" .) -}}
    {{- print "autoscaling/v2beta2" -}}
  {{- else -}}
    {{- print "autoscaling/v2" -}}
  {{- end -}}
{{- end -}}
