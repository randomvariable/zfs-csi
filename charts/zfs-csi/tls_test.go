// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package zfscsi

import (
	"os/exec"
	"strings"
	"testing"
)

func TestTLSRequiresEnabledStorageOwnerWhenNodeAndStorageAreDisabled(t *testing.T) {
	renderChart(t, "--set", "network.tls.enabled=true")
}

func TestTLSRequiresCompleteWorkloadStack(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "network.tls.enabled=true",
		"--set", "node.enabled=true",
		"--set", "node.networkDomain=workers",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
	}, "network.tls.enabled requires node.enabled=true and storage.enabled=true")
	assertRenderFails(t, []string{
		"--set", "network.tls.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-a",
		"--set", "storageNode.networkDomain=fabric-a",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1111",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.portalHost=10.0.0.7",
	}, "network.tls.enabled requires node.enabled=true and storage.enabled=true")
}

func TestTLSSignerUsesSingleOwnerStorageConfiguration(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	output := renderChart(t, args...)
	for _, want := range []string{
		"kind: StatefulSet",
		"name: zfs-csi-tls-signer",
		"--tls-server-leaves=storage-a=10.0.0.7",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("TLS signer missing %q", want)
		}
	}
	for _, signer := range objectsByKind(renderedObjects(t, output), "StatefulSet") {
		assertNodeSelectorValues(t, signer, map[string]string{"kubernetes.io/arch": "amd64"})
	}
}

func TestTLSSignerNodeSelectorMergesUserValues(t *testing.T) {
	args := append(legacyStorageArgs(),
		"--set", "node.enabled=true",
		"--set", "network.tls.enabled=true",
		"--set-string", "network.tls.signer.nodeSelector.consumer-group=signing",
	)
	signers := objectsByKind(renderedObjects(t, renderChart(t, args...)), "StatefulSet")
	if len(signers) != 1 {
		t.Fatalf("TLS signer StatefulSet count = %d, want 1", len(signers))
	}
	assertNodeSelectorValues(t, signers[0], map[string]string{
		"kubernetes.io/arch": "amd64",
		"consumer-group":     "signing",
	})
}

func TestTLSSignerRejectsUnsafeOwnerEndpoint(t *testing.T) {
	args := []string{"--set", "node.enabled=true", "--set", "storage.enabled=true", "--set", "network.tls.enabled=true", "--set", "network.portalHost=10.0.0.7", "--set-json", `storageOwners=[{"name":"storage-a","enabled":true,"nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1"],"poolMountRoot":"/tank","nfs":{"host":"host,other","port":2049},"nvme":{"host":"10.0.0.7","port":4420},"networkDomain":"fabric-a","reachableFrom":["workers"]}]`}
	cmd := exec.Command("helm", append([]string{"template", "zfs-csi", "."}, args...)...)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err == nil || !(strings.Contains(string(output), "nfs.host cannot contain ',' or '='") || strings.Contains(string(output), "values don't meet the specifications of the schema")) {
		t.Fatalf("unsafe signer endpoint render error = %v, output:\n%s", err, output)
	}
}

func TestTLSRequiresEnabledStorageOwnerWhenNodeAndStorageAreEnabled(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "network.tls.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set-json", `storageOwners=[{"name":"storage-off","enabled":false,"nodeSelector":{"owner":"off"},"authoritativePoolGUIDs":["1"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.5","port":2049},"nvme":{"host":"10.0.0.5","port":4420},"networkDomain":"off","reachableFrom":["off"]}]`,
	}, "network.tls.enabled requires at least one enabled storage owner")
}

func TestTLSEnabledRenderIncludesRuntimeWorkloads(t *testing.T) {
	args := append(legacyStorageArgs(),
		"--set", "node.enabled=true",
		"--set", "network.tls.enabled=true",
	)
	output := renderChart(t, args...)
	objects := renderedObjects(t, output)
	assertTLSRuntimeContract(t, objectsByKind(objects, "DaemonSet"), "node", "")
	assertTLSRuntimeContract(t, objectsByKind(objects, "Deployment"), "storage", "--transport-tls=true")
	assertNodeUsesProjectedPodCertificate(t, objectsByKind(objects, "DaemonSet"))
	assertNodeTLSRuntimeUsesNodeConfig(t, objectsByKind(objects, "DaemonSet"))
}

func TestMultiOwnerTLSHasOneDaemonPerStorageHost(t *testing.T) {
	args := []string{
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "network.tls.enabled=true",
		"--set-json", `storageOwners=[{"name":"storage-a","enabled":true,"nodeSelector":{"zfs.csi.randomvariable.co.uk/storage":"storage-a"},"authoritativePoolGUIDs":["1"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.7","port":2049},"nvme":{"host":"10.0.0.7","port":4420},"networkDomain":"fabric-a","reachableFrom":["fabric-a","workers"]}]`,
	}
	objects := renderedObjects(t, renderChart(t, args...))
	for _, daemonSet := range objectsByKind(objects, "DaemonSet") {
		if strings.Contains(marshalObject(t, daemonSet), "app.kubernetes.io/component: node") && strings.Contains(marshalObject(t, daemonSet), "name: tlshd") {
			t.Fatal("multi-owner node DaemonSet must not race storage-owner tlshd")
		}
	}
	for _, deployment := range objectsByKind(objects, "Deployment") {
		text := marshalObject(t, deployment)
		if !strings.Contains(text, "app.kubernetes.io/component: storage") {
			continue
		}
		for _, want := range []string{"name: tlshd", "podCertificate:", "name: tls-client-live", "/run/zfs-csi-tls-client"} {
			if !strings.Contains(text, want) {
				t.Fatalf("storage-owner tlshd missing combined client/server contract %q", want)
			}
		}
		return
	}
	t.Fatal("multi-owner render missing storage deployment")
}

func TestTLSDisabledRenderHasNoRuntimeMaterial(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=false")
	output := renderChart(t, args...)
	for _, unwanted := range []string{
		"name: tlshd",
		"name: tls-ca",
		"name: tls-server-cert",
		"name: tlshd-config",
		"--transport-tls=true",
		"zfs-csi-tlshd-node-config",
		"zfs-csi-tlshd-storage-config",
		"zfs-csi-nfs-tls-secrets",
		"tls-signing-authority",
		"argocd-signing",
	} {
		if strings.Contains(output, unwanted) {
			t.Fatalf("TLS-disabled render contains %q", unwanted)
		}
	}
	for _, namespace := range objectsByKind(renderedObjects(t, output), "Namespace") {
		if objectName(namespace) == "argocd-signing" {
			t.Fatal("TLS-disabled render creates signing Namespace")
		}
	}
}

// tlshd enforces per-file modes: 0600 for the private key and 0644 for the
// certificate. Secret volumes default to 0644 for every file, so the server
// leaf volume must pin each key's mode or every mTLS handshake fails with
// "access denied by server".
func TestStorageServerLeafVolumePinsPrivateKeyMode(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	output := renderChart(t, args...)
	if !strings.Contains(output, "name: tls-server-cert") {
		t.Fatal("storage workload missing tls-server-cert volume")
	}
	for _, want := range []string{"path: tls.key\n                mode: 0600", "path: tls.crt\n                mode: 0644"} {
		if !strings.Contains(output, want) {
			t.Fatalf("tls-server-cert volume must pin per-key modes, missing %q", want)
		}
	}
}

func TestNodeHostUsersRequiresTLS(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=false")
	output := renderChart(t, args...)
	if strings.Contains(output, "hostUsers: true") {
		t.Fatal("TLS-disabled node manifest contains hostUsers")
	}

	args = append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	output = renderChart(t, args...)
	if !strings.Contains(output, "hostUsers: true") {
		t.Fatal("TLS-enabled node manifest omits hostUsers")
	}
}

func assertTLSRuntimeContract(t *testing.T, objects []map[string]any, component, extra string) {
	t.Helper()
	for _, object := range objects {
		text := marshalObject(t, object)
		if !strings.Contains(text, "app.kubernetes.io/component: "+component) {
			continue
		}
		for _, want := range []string{"hostUsers: true", "name: tlshd", "/usr/sbin/tlshd -s -c /etc/tlshd/config", "name: tlshd-config", extra} {
			if want == "" {
				continue
			}
			if !strings.Contains(text, want) {
				t.Fatalf("%s workload missing TLS runtime contract %q", component, want)
			}
		}
		return
	}
	t.Fatalf("TLS-enabled render missing %s runtime workload", component)
}

func assertNodeUsesProjectedPodCertificate(t *testing.T, objects []map[string]any) {
	t.Helper()
	for _, object := range objects {
		text := marshalObject(t, object)
		if !strings.Contains(text, "app.kubernetes.io/component: node") {
			continue
		}
		for _, want := range []string{
			"name: tls-client",
			"podCertificate:",
			"signerName: zfs.csi.randomvariable.co.uk/nfs-client",
			"keyType: ECDSAP256",
			"maxExpirationSeconds: 3600",
			"certificateChainPath: tls.crt",
			"keyPath: tls.key",
			"name: zfs-csi-tls-ca-public",
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("node PodCertificate projection missing %q", want)
			}
		}
		for _, unwanted := range []string{"tls-client-bootstrap", "tls-node-bootstrap", "credentialBundlePath"} {
			if strings.Contains(text, unwanted) {
				t.Fatalf("node TLS manifest contains obsolete credential flow %q", unwanted)
			}
		}
		// The projected files are pinned at 0400 which tlshd rejects, so the
		// container must mirror them into a writable emptyDir with the modes
		// tlshd enforces (0600 key, 0644 cert/CA) and read from there.
		for _, want := range []string{"name: tls-client-live", "/run/zfs-csi-tls-live", "chmod 0600", "chmod 0644"} {
			if !strings.Contains(text, want) {
				t.Fatalf("node tlshd mode-mirror contract missing %q", want)
			}
		}
		return
	}
	t.Fatal("TLS-enabled render missing node DaemonSet")
}

func assertNodeTLSRuntimeUsesNodeConfig(t *testing.T, objects []map[string]any) {
	t.Helper()
	for _, object := range objects {
		text := marshalObject(t, object)
		if !strings.Contains(text, "app.kubernetes.io/component: node") {
			continue
		}
		if !strings.Contains(text, "name: zfs-csi-tlshd-node-config") {
			t.Fatal("node TLS DaemonSet does not mount generated node tlshd ConfigMap")
		}
		if strings.Contains(text, "name: zfs-csi-tlshd-config\n") {
			t.Fatal("node TLS DaemonSet still references obsolete shared tlshd ConfigMap")
		}
		return
	}
	t.Fatal("TLS-enabled render missing node DaemonSet")
}

func TestTLSOnRenderReferencesOnlyGeneratedRoleSpecificConfigs(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	objects := renderedObjects(t, renderChart(t, args...))
	for _, object := range objects {
		text := marshalObject(t, object)
		switch {
		case strings.Contains(text, "app.kubernetes.io/component: node"):
			if !strings.Contains(text, "name: zfs-csi-tlshd-node-config") || strings.Contains(text, "name: zfs-csi-tlshd-config\n") {
				t.Fatalf("node TLS workload has dangling ConfigMap reference:\n%s", text)
			}
		case strings.Contains(text, "app.kubernetes.io/component: storage"):
			if !strings.Contains(text, "name: zfs-csi-tlshd-storage-config") || strings.Contains(text, "name: zfs-csi-tlshd-config\n") {
				t.Fatalf("storage TLS workload has dangling ConfigMap reference:\n%s", text)
			}
		}
	}
}

func TestTLSRuntimeConfigsUseSeparateClientAndServerCredentials(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	objects := renderedObjects(t, renderChart(t, args...))
	configs := objectsByKind(objects, "ConfigMap")
	var nodeConfig, storageConfig string
	for _, object := range configs {
		metadata, _ := object["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		data, _ := object["data"].(map[string]any)
		config, _ := data["config"].(string)
		switch name {
		case "zfs-csi-tlshd-node-config":
			nodeConfig = config
		case "zfs-csi-tlshd-storage-config":
			storageConfig = config
		}
	}
	if !strings.Contains(nodeConfig, "[authenticate.client]") || strings.Contains(nodeConfig, "[authenticate.server]") || strings.Contains(nodeConfig, "/etc/zfs-csi/tls") {
		t.Fatalf("node tlshd config has wrong credential role:\n%s", nodeConfig)
	}
	if !strings.Contains(storageConfig, "[authenticate.server]") || !strings.Contains(storageConfig, "[authenticate.client]") || !strings.Contains(storageConfig, "/run/zfs-csi-tls-ca/ca.crt") || !strings.Contains(storageConfig, "/run/zfs-csi-tls/tls.crt") || !strings.Contains(storageConfig, "/run/zfs-csi-tls-client/tls.crt") {
		t.Fatalf("storage tlshd config has wrong credential role:\n%s", storageConfig)
	}
}

func TestTLSSignerDeploymentOwnsAuthorityLifecycle(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	output := renderChart(t, args...)
	for _, want := range []string{
		"kind: Namespace",
		"app.kubernetes.io/component: tls-signing-authority",
		"name: zfs-csi-tls-signer",
		"namespace: argocd-signing",
		"--mode=tls-signer",
		"--tls-signing-namespace=argocd-signing",
		"--tls-server-leaves=storage-a=10.0.0.7",
		"runAsUser: 65532",
		"runAsGroup: 65532",
		"readOnlyRootFilesystem: true",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("TLS signer render missing %q", want)
		}
	}
	for _, unwanted := range []string{"--mode=tls-bootstrap", "--mode=tls-node-bootstrap", "zfs-csi-tls-bootstrap"} {
		if strings.Contains(output, unwanted) {
			t.Fatalf("obsolete TLS bootstrap rendered: %q", unwanted)
		}
	}
}

func TestTLSSigningNamespaceIsCreatedAndRetained(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true")
	objects := renderedObjects(t, renderChart(t, args...))
	var signingNamespace map[string]any
	for _, object := range objectsByKind(objects, "Namespace") {
		if objectName(object) == "argocd-signing" {
			signingNamespace = object
			break
		}
	}
	if signingNamespace == nil {
		t.Fatal("TLS-enabled render does not create deterministic signing Namespace")
	}
	metadata, _ := signingNamespace["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	if annotations["helm.sh/resource-policy"] != "keep" {
		t.Fatalf("signing Namespace resource policy = %#v, want keep", annotations["helm.sh/resource-policy"])
	}
}

func TestTLSSignerDisabledOmitsSignerAuthority(t *testing.T) {
	args := append(legacyStorageArgs(), "--set", "node.enabled=true", "--set", "network.tls.enabled=true", "--set", "network.tls.signer.enabled=false")
	output := renderChart(t, args...)
	if strings.Contains(output, "--mode=tls-signer") || strings.Contains(output, "name: zfs-csi-tls-signer-ca") {
		t.Fatal("disabled TLS signer rendered signer authority")
	}
}
