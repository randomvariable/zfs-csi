{{/*
Driver image reference. Delegates to common.images.image, which applies the
same digest-over-tag precedence and appVersion fallback this chart has always
used. .Values.image.repository already carries the registry, so no global
registry override is passed.
*/}}
{{- define "zfs-csi.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global "chart" .Chart) -}}
{{- end -}}

{{/*
Namespace the driver components run in. .Values.namespace keeps precedence for
compatibility; common.names.namespace supplies the release-namespace fallback
and the Bitnami namespaceOverride escape hatch.
*/}}
{{- define "zfs-csi.namespace" -}}
{{ .Values.namespace | default (include "common.names.namespace" .) }}
{{- end -}}

{{/*
Standard Bitnami object labels plus this chart's component label.

Deliberately applied to OBJECT metadata only — never to spec.selector.matchLabels
(immutable on Deployment/DaemonSet/StatefulSet) and never to pod template labels
(a helm.sh/chart label changes on every chart version and would roll the node
DaemonSet on unrelated releases). Existing selectors therefore stay byte-identical.

Usage: include "zfs-csi.labels" (dict "context" $ "component" "controller")
*/}}
{{- define "zfs-csi.labels" -}}
{{ include "common.labels.standard" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "zfs-csi.tlsSigningNamespace" -}}
{{- $driver := include "zfs-csi.namespace" . -}}
{{- $configured := .Values.network.tls.signingNamespace | default (printf "%s-signing" $driver) -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$" $configured) -}}
{{- fail "network.tls.signingNamespace must be a DNS1123 subdomain" -}}
{{- end -}}
{{- if eq $driver $configured -}}
{{- fail "network.tls.signingNamespace must differ from the driver namespace" -}}
{{- end -}}
{{- $configured -}}
{{- end -}}

{{- define "zfs-csi.nodeSelector" -}}
nodeSelector:
{{- toYaml .Values.storageNode.selector | nindent 2 }}
{{- end -}}

{{- define "zfs-csi.tolerations" -}}
tolerations:
{{- toYaml .Values.storageNode.tolerations | nindent 2 }}
{{- end -}}

{{- define "zfs-csi.ownerResourceName" -}}
{{- $slug := regexReplaceAll "[^a-z0-9-]+" (lower .name) "-" | trimAll "-" | trunc 40 | trimSuffix "-" -}}
{{- printf "zfs-csi-storage-%s-%s" $slug (sha256sum .name | trunc 8) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Must match tlsca.ServerSecretName: owner names are DNS subdomains. */}}
{{- define "zfs-csi.tlsServerSecretName" -}}
{{- printf "zfs-csi-tls-server-%s" . -}}
{{- end -}}

{{- define "zfs-csi.validateStorageOwners" -}}
{{- $nfsEnabled := or .Values.storageClasses.tankNFS.enabled .Values.storageClasses.tankNFSTLS.enabled .Values.storageClasses.flashNFS.enabled -}}
{{- if and $nfsEnabled .Values.storageNode.enabled (eq (len .Values.storageOwners) 0) (gt (len .Values.storageNode.authoritativePoolGUIDs) 1) -}}
{{- fail "legacy storage owner storageNode configures multiple authoritative pools while an NFS filesystem StorageClass is enabled; this release supports one NFS-exportable pool root per owner because one host nfsd has one fsid=0 pseudoroot; split pools across owners/endpoints or disable all chart NFS StorageClasses" -}}
{{- end -}}
{{- if gt (len .Values.storageOwners) 0 -}}
{{- if .Values.storageNode.name -}}
{{- fail "storageOwners cannot be combined with legacy storageNode.name" -}}
{{- end -}}
{{- $ownerNames := dict -}}
{{- $poolGUIDOwners := dict -}}
{{- $endpointOwners := dict -}}
{{- $selectorOwners := dict -}}
{{- range $owner := .Values.storageOwners -}}
{{- $name := required "storage owner name is required" $owner.name -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$" $name) -}}
{{- fail (printf "storage owner %q must be a DNS subdomain for its TLS leaf Secret" $name) -}}
{{- end -}}
{{- if eq (len $owner.nodeSelector) 0 -}}
{{- fail (printf "storage owner %q nodeSelector must not be empty" $name) -}}
{{- end -}}
{{- if not (has $owner.networkDomain $owner.reachableFrom) -}}
{{- fail (printf "storage owner %q reachableFrom must include networkDomain %q" $name $owner.networkDomain) -}}
{{- end -}}
{{- if hasKey $ownerNames $name -}}
{{- fail (printf "duplicate storage owner name %q" $name) -}}
{{- end -}}
{{- $_ := set $ownerNames $name true -}}
{{- $selector := toJson $owner.nodeSelector -}}
{{- if hasKey $selectorOwners $selector -}}
{{- fail (printf "node selector %s is assigned to both storage owners %q and %q" $selector (get $selectorOwners $selector) $name) -}}
{{- end -}}
{{- $_ := set $selectorOwners $selector $name -}}
{{- range $guid := $owner.authoritativePoolGUIDs -}}
{{- if hasKey $poolGUIDOwners $guid -}}
{{- fail (printf "authoritative pool GUID %q is assigned to both storage owners %q and %q" $guid (get $poolGUIDOwners $guid) $name) -}}
{{- end -}}
{{- $_ := set $poolGUIDOwners $guid $name -}}
{{- end -}}
{{- $enabled := true -}}
{{- if hasKey $owner "enabled" -}}{{- $enabled = $owner.enabled -}}{{- end -}}
{{- if and $nfsEnabled $enabled (gt (len $owner.authoritativePoolGUIDs) 1) -}}
{{- fail (printf "storage owner %q configures multiple authoritative pools while an NFS filesystem StorageClass is enabled; this release supports one NFS-exportable pool root per owner because one host nfsd has one fsid=0 pseudoroot; split pools across owners/endpoints or disable all chart NFS StorageClasses" $name) -}}
{{- end -}}
{{- if $enabled -}}
{{- range $endpoint := list (printf "%s|%v" (lower $owner.nfs.host) $owner.nfs.port) (printf "%s|%v" (lower $owner.nvme.host) $owner.nvme.port) -}}
{{- if hasKey $endpointOwners $endpoint -}}
{{- fail (printf "unsafe endpoint collision %q between storage owners %q and %q" $endpoint (get $endpointOwners $endpoint) $name) -}}
{{- end -}}
{{- $_ := set $endpointOwners $endpoint $name -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Chart-generated StorageClass keys mapped to their values entry. Keeps the
default-class selector and the StorageClass templates working from one list.
*/}}
{{- define "zfs-csi.storageClassKeys" -}}
{{- print "tankNVMe tankNFS tankNFSTLS tankNVMeTLS flashNVMe flashNFS" -}}
{{- end -}}

{{/*
Reports whether one chart StorageClass key/variant pair actually renders, using
the same guards as templates/storageclasses.yaml. Returns "true" or "".
Usage: include "zfs-csi.storageClassRenders" (dict "context" $ "key" "tankNVMe" "variant" "encrypted")
*/}}
{{- define "zfs-csi.storageClassRenders" -}}
{{- $values := .context.Values -}}
{{- $class := get $values.storageClasses .key -}}
{{- $renders := and $values.controller.enabled $class.enabled -}}
{{- if or (eq .key "tankNFSTLS") (eq .key "tankNVMeTLS") -}}
{{- $renders = and $renders $values.network.tls.enabled $values.node.enabled $values.storage.enabled -}}
{{- end -}}
{{- if eq .variant "encrypted" -}}
{{- $renders = and $renders $values.encryption.enabled -}}
{{- end -}}
{{- if $renders -}}true{{- end -}}
{{- end -}}

{{/*
Validates storageClasses.defaultClass / defaultClassVariant. An empty
defaultClass keeps the chart's historical behaviour: no chart StorageClass is
ever marked as the cluster default, so unrelated PVCs are untouched.
*/}}
{{- define "zfs-csi.validateDefaultStorageClass" -}}
{{- $key := .Values.storageClasses.defaultClass | default "" -}}
{{- $variant := .Values.storageClasses.defaultClassVariant | default "plain" -}}
{{- if not (has $variant (list "plain" "encrypted")) -}}
{{- fail (printf "storageClasses.defaultClassVariant must be \"plain\" or \"encrypted\", got %q" $variant) -}}
{{- end -}}
{{- if $key -}}
{{- $known := splitList " " (include "zfs-csi.storageClassKeys" .) -}}
{{- if not (has $key $known) -}}
{{- fail (printf "storageClasses.defaultClass %q is not a chart StorageClass; valid keys are %s" $key (join ", " $known)) -}}
{{- end -}}
{{- $class := get .Values.storageClasses $key -}}
{{- if not $class.enabled -}}
{{- fail (printf "storageClasses.defaultClass %q requires storageClasses.%s.enabled=true" $key $key) -}}
{{- end -}}
{{- if and (eq $variant "encrypted") (not .Values.encryption.enabled) -}}
{{- fail (printf "storageClasses.defaultClass %q with defaultClassVariant=encrypted requires encryption.enabled=true" $key) -}}
{{- end -}}
{{- if not (include "zfs-csi.storageClassRenders" (dict "context" . "key" $key "variant" $variant)) -}}
{{- fail (printf "storageClasses.defaultClass %q (%s variant) is not rendered by this release; enable controller.enabled and, for TLS classes, network.tls.enabled with node.enabled and storage.enabled" $key $variant) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Emits the Kubernetes default-StorageClass annotation for exactly the one
selected class/variant, and nothing otherwise. Callers place it directly under
the StorageClass metadata name.
Usage: include "zfs-csi.storageClassDefaultAnnotation" (dict "context" $ "key" "tankNVMe" "variant" "plain")
*/}}
{{- define "zfs-csi.storageClassDefaultAnnotation" -}}
{{- $values := .context.Values -}}
{{- $selected := $values.storageClasses.defaultClass | default "" -}}
{{- $variant := $values.storageClasses.defaultClassVariant | default "plain" -}}
{{- if and $selected (eq $selected .key) (eq $variant .variant) }}
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
{{- end -}}
{{- end -}}
