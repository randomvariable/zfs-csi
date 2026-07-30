// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package zfscsi

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRenderedClusterRolesGrantV2EventWrites(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatalf("read RBAC template: %v", err)
	}

	roles := templateClusterRoles(t, manifest)
	for _, name := range []string{"zfs-csi-controller", "nvmet-controller"} {
		role, ok := roles[name]
		if !ok {
			t.Fatalf("ClusterRole %q missing from rendered chart", name)
		}
		if !grantsV2EventWrites(role) {
			t.Fatalf("ClusterRole %q lacks events.k8s.io/events create/update/patch", name)
		}
	}
}

func TestControllerRoleGrantsVolumeAttributesClassRead(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatalf("read RBAC template: %v", err)
	}

	role := templateClusterRoles(t, manifest)["zfs-csi-controller"]
	if role == nil {
		t.Fatal("zfs-csi-controller ClusterRole missing")
	}
	if !grantsStandaloneVolumeAttributesClassRead(role) {
		t.Fatal("zfs-csi-controller lacks storage.k8s.io/volumeattributesclasses get/list/watch")
	}
}

func TestControllerRoleGrantsHealthMonitorPVCStatusWrites(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatalf("read RBAC template: %v", err)
	}
	role := templateClusterRoles(t, manifest)["zfs-csi-controller"]
	if role == nil || !grantsPVCStatusWrites(role) {
		t.Fatal("health monitor needs to patch PVC status")
	}
}

func TestControllerRoleGrantsRuntimeVolumeImportReconciliationOnly(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	role := templateClusterRoles(t, manifest)["zfs-csi-controller"]
	if role == nil {
		t.Fatal("controller ClusterRole missing")
	}
	rules, _ := role["rules"].([]any)
	found := false
	for _, raw := range rules {
		rule, _ := raw.(map[string]any)
		if hasString(rule["resources"], "volumeimports") && hasString(rule["verbs"], "create") {
			t.Fatal("runtime role must not grant VolumeImport creation")
		}
		if hasString(rule["resources"], "volumeimports/status") && hasString(rule["verbs"], "get") && hasString(rule["verbs"], "patch") {
			found = true
		}
	}
	if !found {
		t.Fatal("runtime role lacks VolumeImport reconciliation permissions")
	}
}

func TestStorageNodeRBACIsStatusOnlyForStorageAgent(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	roles := templateClusterRoles(t, manifest)
	storage := roles["zfs-csi-storage"]
	controller := roles["zfs-csi-controller"]
	if storage == nil || controller == nil {
		t.Fatal("storage/controller ClusterRole missing")
	}
	for _, raw := range storage["rules"].([]any) {
		rule := raw.(map[string]any)
		if hasString(rule["resources"], "storagenodes") && (hasString(rule["verbs"], "create") || hasString(rule["verbs"], "update") || hasString(rule["verbs"], "patch") || hasString(rule["verbs"], "delete")) {
			t.Fatal("storage role can mutate StorageNode spec/object")
		}
	}
	foundStatus := false
	for _, raw := range storage["rules"].([]any) {
		rule := raw.(map[string]any)
		if hasString(rule["resources"], "storagenodes/status") && hasString(rule["verbs"], "update") && hasString(rule["verbs"], "patch") {
			foundStatus = true
		}
	}
	if !foundStatus {
		t.Fatal("storage role lacks StorageNode status update/patch")
	}
	for _, raw := range controller["rules"].([]any) {
		rule := raw.(map[string]any)
		if hasString(rule["resources"], "storagenodes") && (hasString(rule["verbs"], "create") || hasString(rule["verbs"], "update") || hasString(rule["verbs"], "patch") || hasString(rule["verbs"], "delete")) {
			t.Fatal("controller role can mutate StorageNodes")
		}
	}
}

func TestNodeRoleGrantsOnlyNodeGet(t *testing.T) {
	manifest, err := os.ReadFile("templates/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	role := templateClusterRoles(t, manifest)["zfs-csi-node"]
	if role == nil {
		t.Fatal("node ClusterRole missing")
	}
	rules, ok := role["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("node rules = %#v, want one rule", role["rules"])
	}
	rule := rules[0].(map[string]any)
	if !onlyString(rule["apiGroups"], "") || !onlyString(rule["resources"], "nodes") || !onlyStrings(rule["verbs"], "get") {
		t.Fatalf("node role rule = %#v, want core/nodes get only", rule)
	}

	output := renderChart(t)
	if !strings.Contains(output, "kind: ClusterRole\nmetadata:\n  name: zfs-csi-node") ||
		!strings.Contains(output, "name: zfs-csi-node\nroleRef:") {
		t.Fatal("default render is missing node ClusterRole or ClusterRoleBinding")
	}
	start := strings.Index(output, "kind: ClusterRole\nmetadata:\n  name: zfs-csi-node")
	if start < 0 {
		t.Fatal("rendered node ClusterRole missing")
	}
	section := output[start:]
	if end := strings.Index(section, "\n---\n"); end >= 0 {
		section = section[:end]
	}
	if strings.Contains(section, "list") || strings.Contains(section, "watch") {
		t.Fatalf("rendered node ClusterRole grants list/watch:\n%s", section)
	}
}

func TestControllerNamespacedRoleGrantsOnlyNVMePSKSecretCreateAndGet(t *testing.T) {
	const (
		roleName        = "zfs-csi-nvme-tls-psk-secrets"
		driverNamespace = "argocd"
	)
	objects := renderedRBACObjects(t)
	role := rbacObject(objects, "Role", roleName)
	if role == nil {
		t.Fatal("namespaced NVMe TLS PSK Role missing")
	}
	if got := objectNamespace(role); got != driverNamespace {
		t.Fatalf("NVMe TLS PSK Role namespace = %q, want %q", got, driverNamespace)
	}
	rules, ok := role["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("NVMe TLS PSK Role rules = %#v, want exactly one", role["rules"])
	}
	rule := rules[0].(map[string]any)
	if !onlyString(rule["apiGroups"], "") || !onlyString(rule["resources"], "secrets") || !onlyStrings(rule["verbs"], "get", "create") {
		t.Fatalf("NVMe TLS PSK Role rule = %#v, want core/secrets get/create only", rule)
	}

	binding := rbacObject(objects, "RoleBinding", roleName)
	if binding == nil {
		t.Fatal("NVMe TLS PSK RoleBinding missing")
	}
	if got := objectNamespace(binding); got != driverNamespace {
		t.Fatalf("NVMe TLS PSK RoleBinding namespace = %q, want %q", got, driverNamespace)
	}
	roleRef := binding["roleRef"].(map[string]any)
	if roleRef["apiGroup"] != "rbac.authorization.k8s.io" || roleRef["kind"] != "Role" || roleRef["name"] != roleName {
		t.Fatalf("NVMe TLS PSK RoleBinding roleRef = %#v", roleRef)
	}
	subjects, ok := binding["subjects"].([]any)
	if !ok || len(subjects) != 1 {
		t.Fatalf("NVMe TLS PSK RoleBinding subjects = %#v, want controller only", binding["subjects"])
	}
	subject := subjects[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "zfs-csi-controller" || subject["namespace"] != driverNamespace {
		t.Fatalf("NVMe TLS PSK RoleBinding subject = %#v, want controller SA in driver namespace", subject)
	}

	for _, object := range objects {
		if object["kind"] != "Role" || (rbacObjectName(object) == roleName || rbacObjectName(object) == "zfs-csi-storage-nvme-tls-psk-secrets" || rbacObjectName(object) == "zfs-csi-node-nvme-tls-psk-secrets" || rbacObjectName(object) == "zfs-csi-nfs-tls-secrets" || rbacObjectName(object) == "zfs-csi-node-nfs-tls-secrets" || rbacObjectName(object) == "zfs-csi-tls-signer-public") || objectNamespace(object) != driverNamespace {
			continue
		}
		for _, raw := range object["rules"].([]any) {
			if hasString(raw.(map[string]any)["resources"], "secrets") {
				t.Fatalf("Role %q also grants Secret access: %#v", rbacObjectName(object), raw)
			}
		}
	}
}

func TestStorageNamespacedRoleGrantsOnlyNVMePSKSecretGetAndDelete(t *testing.T) {
	const (
		roleName        = "zfs-csi-storage-nvme-tls-psk-secrets"
		driverNamespace = "argocd"
	)
	objects := renderedRBACObjects(t)
	role := rbacObject(objects, "Role", roleName)
	if role == nil || objectNamespace(role) != driverNamespace {
		t.Fatalf("storage NVMe TLS PSK Role missing or wrong namespace: %#v", role)
	}
	rules, ok := role["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("storage NVMe TLS PSK Role rules = %#v, want exactly one", role["rules"])
	}
	rule := rules[0].(map[string]any)
	if !onlyString(rule["apiGroups"], "") || !onlyString(rule["resources"], "secrets") || !onlyStrings(rule["verbs"], "get", "delete") {
		t.Fatalf("storage NVMe TLS PSK Role rule = %#v, want core/secrets get/delete only", rule)
	}

	binding := rbacObject(objects, "RoleBinding", roleName)
	if binding == nil || objectNamespace(binding) != driverNamespace {
		t.Fatalf("storage NVMe TLS PSK RoleBinding missing or wrong namespace: %#v", binding)
	}
	roleRef := binding["roleRef"].(map[string]any)
	if roleRef["apiGroup"] != "rbac.authorization.k8s.io" || roleRef["kind"] != "Role" || roleRef["name"] != roleName {
		t.Fatalf("storage NVMe TLS PSK RoleBinding roleRef = %#v", roleRef)
	}
	subjects, ok := binding["subjects"].([]any)
	if !ok || len(subjects) != 1 {
		t.Fatalf("storage NVMe TLS PSK RoleBinding subjects = %#v", binding["subjects"])
	}
	subject := subjects[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "zfs-csi-storage" || subject["namespace"] != driverNamespace {
		t.Fatalf("storage NVMe TLS PSK RoleBinding subject = %#v", subject)
	}
}

func TestNodeNamespacedRoleGrantsOnlyNVMePSKSecretGet(t *testing.T) {
	const (
		roleName        = "zfs-csi-node-nvme-tls-psk-secrets"
		driverNamespace = "argocd"
	)
	objects := renderedRBACObjects(t)
	role := rbacObject(objects, "Role", roleName)
	if role == nil || objectNamespace(role) != driverNamespace {
		t.Fatalf("node NVMe TLS PSK Role missing or wrong namespace: %#v", role)
	}
	rules, ok := role["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("node NVMe TLS PSK Role rules = %#v, want exactly one", role["rules"])
	}
	rule := rules[0].(map[string]any)
	if !onlyString(rule["apiGroups"], "") || !onlyString(rule["resources"], "secrets") || !onlyString(rule["verbs"], "get") {
		t.Fatalf("node NVMe TLS PSK Role rule = %#v, want core/secrets get only", rule)
	}
	binding := rbacObject(objects, "RoleBinding", roleName)
	if binding == nil || objectNamespace(binding) != driverNamespace {
		t.Fatalf("node NVMe TLS PSK RoleBinding missing or wrong namespace: %#v", binding)
	}
	roleRef := binding["roleRef"].(map[string]any)
	if roleRef["apiGroup"] != "rbac.authorization.k8s.io" || roleRef["kind"] != "Role" || roleRef["name"] != roleName {
		t.Fatalf("node NVMe TLS PSK RoleBinding roleRef = %#v", roleRef)
	}
	subjects, ok := binding["subjects"].([]any)
	if !ok || len(subjects) != 1 {
		t.Fatalf("node NVMe TLS PSK RoleBinding subjects = %#v", binding["subjects"])
	}
	subject := subjects[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "zfs-csi-node" || subject["namespace"] != driverNamespace {
		t.Fatalf("node NVMe TLS PSK RoleBinding subject = %#v", subject)
	}
}

func TestNFSTLSRoleIsOwnerScopedAndLeastPrivilege(t *testing.T) {
	objects := renderedRBACObjects(t,
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set-json", `storageOwners=[{"name":"storage-a","enabled":true,"nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.5","port":2049},"nvme":{"host":"10.0.0.5","port":4420},"networkDomain":"a","reachableFrom":["a"]},{"name":"storage-b","enabled":true,"nodeSelector":{"owner":"b"},"authoritativePoolGUIDs":["2"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.9","port":2049},"nvme":{"host":"10.0.0.9","port":4420},"networkDomain":"b","reachableFrom":["b"]}]`,
		"--set", "network.tls.enabled=true",
	)
	role := rbacObject(objects, "Role", "zfs-csi-nfs-tls-secrets")
	if role == nil {
		t.Fatal("NFS TLS Role missing")
	}
	rules := role["rules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("NFS TLS Role rules = %#v, want certificate-only get", rules)
	}
	read := rules[0].(map[string]any)
	if !onlyString(read["apiGroups"], "") || !onlyString(read["resources"], "secrets") || !onlyString(read["verbs"], "get") || !onlyStrings(read["resourceNames"], "zfs-csi-tls-ca-public", "zfs-csi-tls-server-storage-a", "zfs-csi-tls-server-storage-b") {
		t.Fatalf("NFS TLS get rule = %#v", read)
	}
	binding := rbacObject(objects, "RoleBinding", "zfs-csi-nfs-tls-secrets")
	if binding == nil {
		t.Fatal("NFS TLS RoleBinding missing")
	}
	subject := binding["subjects"].([]any)[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "zfs-csi-storage" {
		t.Fatalf("NFS TLS RoleBinding subject = %#v", subject)
	}
}

func TestNFSTLSRoleExcludesDisabledOwnerLeaf(t *testing.T) {
	objects := renderedRBACObjects(t, "--set", "network.tls.enabled=true", "--set", "storage.enabled=true", "--set", "node.enabled=true", "--set-json", `storageOwners=[{"name":"storage-a","nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.5","port":2049},"nvme":{"host":"10.0.0.5","port":4420},"networkDomain":"a","reachableFrom":["a"]},{"name":"storage-off","enabled":false,"nodeSelector":{"owner":"off"},"authoritativePoolGUIDs":["2"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.6","port":2049},"nvme":{"host":"10.0.0.6","port":4420},"networkDomain":"off","reachableFrom":["off"]}]`)
	role := rbacObject(objects, "Role", "zfs-csi-nfs-tls-secrets")
	if role == nil {
		t.Fatal("NFS TLS role missing")
	}
	rules := role["rules"].([]any)
	if !onlyStrings(rules[0].(map[string]any)["resourceNames"], "zfs-csi-tls-ca-public", "zfs-csi-tls-server-storage-a") {
		t.Fatalf("TLS resourceNames = %#v", rules[0].(map[string]any)["resourceNames"])
	}
}

func TestTLSSignerAloneCanReadPrivateCA(t *testing.T) {
	objects := renderedRBACObjects(t,
		"--set", "network.tls.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storageNode.name=storage-a",
		"--set", "storageNode.networkDomain=workers",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.5",
		"--set", "network.nfsServer=10.0.0.5",
	)
	role := rbacObject(objects, "Role", "zfs-csi-tls-signer-ca")
	if role == nil || objectNamespace(role) != "argocd-signing" {
		t.Fatalf("signer CA Role = %#v", role)
	}
	rules := role["rules"].([]any)
	if !ruleGrantsNamedSecret(rules, "zfs-csi-tls-ca") {
		t.Fatalf("signer CA Role does not grant named private Secret: %#v", rules)
	}
	for _, object := range objects {
		if object["kind"] != "Role" || rbacObjectName(object) == "zfs-csi-tls-signer-ca" || rbacObjectName(object) == "zfs-csi-tls-signer-public" {
			continue
		}
		if rules, ok := object["rules"].([]any); ok && ruleGrantsNamedSecret(rules, "zfs-csi-tls-ca") {
			t.Fatalf("Role %q also grants private CA access", rbacObjectName(object))
		}
	}
	binding := rbacObject(objects, "RoleBinding", "zfs-csi-tls-signer-ca")
	subject := binding["subjects"].([]any)[0].(map[string]any)
	if subject["name"] != "zfs-csi-tls-signer" || subject["namespace"] != "argocd-signing" {
		t.Fatalf("private CA binding subject = %#v", subject)
	}
	publicRole := rbacObject(objects, "Role", "zfs-csi-tls-signer-public")
	foundUnscopedCreate := false
	for _, raw := range publicRole["rules"].([]any) {
		rule := raw.(map[string]any)
		if hasString(rule["resources"], "secrets") && onlyString(rule["verbs"], "create") && rule["resourceNames"] == nil {
			foundUnscopedCreate = true
		}
	}
	if !foundUnscopedCreate {
		t.Fatalf("signer public role lacks unscoped Secret create rule: %#v", publicRole["rules"])
	}
}

func TestLegacyNFSClientSecretRBACIsAbsent(t *testing.T) {
	objects := renderedRBACObjects(t,
		"--set", "network.tls.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storageNode.name=storage-a",
		"--set", "storageNode.networkDomain=workers",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.5",
		"--set", "network.nfsServer=10.0.0.5",
	)
	for _, name := range []string{"zfs-csi-controller-nfs-client-tls-secrets", "zfs-csi-node-nfs-client-tls-secrets", "zfs-csi-tls-bootstrap"} {
		if rbacObject(objects, "Role", name) != nil || rbacObject(objects, "RoleBinding", name) != nil {
			t.Fatalf("legacy NFS TLS object %q still rendered", name)
		}
	}
	nodeRole := rbacObject(objects, "Role", "zfs-csi-node-nfs-tls-secrets")
	rule := nodeRole["rules"].([]any)[0].(map[string]any)
	if !onlyString(rule["verbs"], "get") || !onlyString(rule["resourceNames"], "zfs-csi-tls-ca-public") {
		t.Fatalf("node public CA rule = %#v", rule)
	}
}

func ruleGrantsNamedSecret(rules []any, name string) bool {
	for _, raw := range rules {
		rule, ok := raw.(map[string]any)
		if ok && onlyString(rule["resources"], "secrets") && hasString(rule["resourceNames"], name) && hasString(rule["verbs"], "get") {
			return true
		}
	}
	return false
}

func renderedRBACObjects(t *testing.T, args ...string) []map[string]any {
	t.Helper()
	var objects []map[string]any
	for document := range bytes.SplitSeq([]byte(renderChart(t, args...)), []byte("\n---\n")) {
		if !bytes.Contains(document, []byte("\nkind: Role\n")) && !bytes.Contains(document, []byte("\nkind: RoleBinding\n")) {
			continue
		}
		var object map[string]any
		if err := yaml.Unmarshal(document, &object); err != nil {
			t.Fatalf("decode rendered RBAC object: %v", err)
		}
		objects = append(objects, object)
	}
	return objects
}

func rbacObject(objects []map[string]any, kind, name string) map[string]any {
	for _, object := range objects {
		if object["kind"] == kind && rbacObjectName(object) == name {
			return object
		}
	}
	return nil
}

func rbacObjectName(object map[string]any) string {
	metadata, _ := object["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}

func objectNamespace(object map[string]any) string {
	metadata, _ := object["metadata"].(map[string]any)
	namespace, _ := metadata["namespace"].(string)
	return namespace
}

func templateClusterRoles(t *testing.T, manifest []byte) map[string]map[string]any {
	t.Helper()

	roles := map[string]map[string]any{}
	for document := range bytes.SplitSeq(manifest, []byte("\n---\n")) {
		// Other documents contain Helm actions, but ClusterRole documents do not.
		if !bytes.Contains(document, []byte("\nkind: ClusterRole\n")) {
			continue
		}

		var object map[string]any
		if err := yaml.Unmarshal(document, &object); err != nil {
			t.Fatalf("decode ClusterRole template: %v", err)
		}
		metadata, _ := object["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		roles[name] = object
	}

	return roles
}

func grantsV2EventWrites(role map[string]any) bool {
	rules, ok := role["rules"].([]any)
	if !ok {
		return false
	}
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		if hasString(rule["apiGroups"], "events.k8s.io") && hasString(rule["resources"], "events") &&
			hasString(rule["verbs"], "create") && hasString(rule["verbs"], "update") && hasString(rule["verbs"], "patch") {
			return true
		}
	}

	return false
}

func grantsStandaloneVolumeAttributesClassRead(role map[string]any) bool {
	rules, ok := role["rules"].([]any)
	if !ok {
		return false
	}
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		if hasString(rule["apiGroups"], "storage.k8s.io") && onlyString(rule["resources"], "volumeattributesclasses") &&
			onlyStrings(rule["verbs"], "get", "list", "watch") {
			return true
		}
	}

	return false
}

func onlyString(raw any, want string) bool {
	values, ok := raw.([]any)
	return ok && len(values) == 1 && values[0] == want
}

func onlyStrings(raw any, want ...string) bool {
	values, ok := raw.([]any)
	if !ok || len(values) != len(want) {
		return false
	}
	for _, value := range want {
		if !hasString(values, value) {
			return false
		}
	}
	return true
}

func grantsPVCStatusWrites(role map[string]any) bool {
	rules, ok := role["rules"].([]any)
	if !ok {
		return false
	}
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		if hasString(rule["apiGroups"], "") && hasString(rule["resources"], "persistentvolumeclaims/status") &&
			hasString(rule["verbs"], "get") && hasString(rule["verbs"], "patch") && hasString(rule["verbs"], "update") {
			return true
		}
	}
	return false
}

func hasString(raw any, want string) bool {
	values, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}
