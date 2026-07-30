{{- define "zfs-csi.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{- define "zfs-csi.namespace" -}}
{{ .Values.namespace | default .Release.Namespace }}
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
