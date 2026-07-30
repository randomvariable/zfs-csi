// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package zfscsi

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLegacyStorageOwnerSynthesis(t *testing.T) {
	output := renderChart(t, legacyStorageArgs()...)
	for _, want := range []string{
		"kind: Deployment",
		"strategy:\n    type: Recreate",
		"--expected-owner=storage-a",
		"--reachable-from=fabric-a",
		"--portal-host=10.0.0.7:4420",
		"--nfs-server=10.0.0.7",
		"kind: StorageNode",
		"helm.sh/resource-policy: keep",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("legacy storage render missing %q", want)
		}
	}
	if strings.Contains(output, "kind: DaemonSet\nmetadata:\n  name: zfs-csi-storage") {
		t.Fatal("legacy storage owner must render a Deployment, not the old DaemonSet")
	}
	for _, deployment := range objectsByKind(renderedObjects(t, output), "Deployment") {
		if strings.HasPrefix(objectName(deployment), "zfs-csi-storage") {
			assertNodeSelectorValues(t, deployment, map[string]string{
				"kubernetes.io/arch":                   "amd64",
				"zfs.csi.randomvariable.co.uk/storage": "true",
			})
		}
	}
}

func TestStorageOwnerNodeSelectorDefaultsAndPreservesOwnerSelector(t *testing.T) {
	for _, deployment := range objectsByKind(renderedObjects(t, renderChart(t, multiOwnerArgs(true, false)...)), "Deployment") {
		name := objectName(deployment)
		if !strings.HasPrefix(name, "zfs-csi-storage-") {
			continue
		}
		owner := deployment["metadata"].(map[string]any)["labels"].(map[string]any)["zfs.csi.randomvariable.co.uk/owner"].(string)
		assertNodeSelectorValues(t, deployment, map[string]string{
			"kubernetes.io/arch":                         "amd64",
			"zfs.csi.randomvariable.co.uk/storage-owner": owner,
		})
	}
}

// TestStorageAgentResponderAccess locks the in-process nfsd responder's kernel
// access prerequisite: the storage agent must run with hostNetwork (so
// /proc/net/rpc reflects the host's sunrpc/nfsd cache channels) and privileged
// (so it can open and write those channels). The responder is the sole NFS
// export mechanism, so losing either setting would silently break every
// filesystem export.
func TestStorageAgentResponderAccess(t *testing.T) {
	output := renderChart(t, legacyStorageArgs()...)
	objects := renderedObjects(t, output)
	checked := 0
	for _, deployment := range objectsByKind(objects, "Deployment") {
		name := objectName(deployment)
		if !strings.HasPrefix(name, "zfs-csi-storage") {
			continue // controller/other deployments are not the storage agent
		}
		checked++
		text := marshalObject(t, deployment)
		for _, want := range []string{"hostNetwork: true", "privileged: true"} {
			if !strings.Contains(text, want) {
				t.Fatalf("storage Deployment %s missing %q (nfsd responder needs host net ns + privileged /proc/net/rpc access)", name, want)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no zfs-csi-storage Deployment rendered to check responder access")
	}
}

func TestStorageAgentLoadsNVMeTargetModules(t *testing.T) {
	output := renderChart(t, legacyStorageArgs()...)
	for _, deployment := range objectsByKind(renderedObjects(t, output), "Deployment") {
		if !strings.HasPrefix(objectName(deployment), "zfs-csi-storage") {
			continue
		}
		podSpec := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		initContainers := podSpec["initContainers"].([]any)
		if len(initContainers) != 1 {
			t.Fatalf("storage Deployment %s has incorrect module loader: %#v", objectName(deployment), initContainers)
		}
		loader := marshalObject(t, initContainers[0].(map[string]any))
		if !strings.Contains(loader, "modprobe nvme-keyring") || !strings.Contains(loader, "modprobe nvmet") || !strings.Contains(loader, "modprobe nvmet-tcp") {
			t.Fatalf("storage Deployment %s has incorrect module loader: %s", objectName(deployment), loader)
		}
		for _, want := range []string{"privileged: true", "mountPath: /lib/modules", "readOnly: true", "mountPath: /sys"} {
			if !strings.Contains(loader, want) {
				t.Fatalf("storage Deployment %s loader missing %q: %s", objectName(deployment), want, loader)
			}
		}
		return
	}
	t.Fatal("no storage Deployment rendered")
}

func TestStorageAgentRendersAllConfiguredOwnerTolerations(t *testing.T) {
	output := renderChart(t, append(legacyStorageArgs(),
		"--set-json", `storageNode.tolerations=[{"key":"zfs.csi.randomvariable.co.uk/storage","operator":"Equal","value":"true","effect":"NoSchedule"},{"key":"node-role.kubernetes.io/nas","operator":"Exists","effect":"NoSchedule"}]`,
	)...)
	objects := renderedObjects(t, output)
	for _, deployment := range objectsByKind(objects, "Deployment") {
		if !strings.HasPrefix(objectName(deployment), "zfs-csi-storage") {
			continue
		}
		podSpec := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		tolerations := marshalObject(t, map[string]any{"tolerations": podSpec["tolerations"]})
		for _, key := range []string{"zfs.csi.randomvariable.co.uk/storage", "node-role.kubernetes.io/nas"} {
			if !strings.Contains(tolerations, "key: "+key) {
				t.Fatalf("storage Deployment %s missing configured toleration %q: %s", objectName(deployment), key, tolerations)
			}
		}
		return
	}
	t.Fatal("no storage Deployment rendered")
}

func TestStorageAgentNFSShutdownHook(t *testing.T) {
	output := renderChart(t, legacyStorageArgs()...)
	objects := renderedObjects(t, output)
	for _, deployment := range objectsByKind(objects, "Deployment") {
		if !strings.HasPrefix(objectName(deployment), "zfs-csi-storage") {
			continue
		}
		spec := deployment["spec"].(map[string]any)
		podSpec := spec["template"].(map[string]any)["spec"].(map[string]any)
		if podSpec["terminationGracePeriodSeconds"] != 45 {
			t.Fatalf("storage Deployment %s termination grace = %v, want 45", objectName(deployment), podSpec["terminationGracePeriodSeconds"])
		}
		if hostPID, found := podSpec["hostPID"]; found && hostPID == true {
			t.Fatalf("storage Deployment %s must not use hostPID", objectName(deployment))
		}
		containers := podSpec["containers"].([]any)
		storage := containers[0].(map[string]any)
		lifecycle := storage["lifecycle"].(map[string]any)
		preStop := lifecycle["preStop"].(map[string]any)["exec"].(map[string]any)
		command := preStop["command"].([]any)
		if strings.Join([]string{command[0].(string), command[1].(string), command[2].(string)}, " ") != "/bin/sh -c kill -TERM 1" {
			t.Fatalf("storage Deployment %s preStop command = %v", objectName(deployment), command)
		}
		return
	}
	t.Fatal("no storage Deployment rendered")
}

func TestMultiOwnerRendersIsolatedDeploymentsAndStorageNodes(t *testing.T) {
	output := renderChart(t, multiOwnerArgs(true, false)...)
	objects := renderedObjects(t, output)
	deployments := objectsByKind(objects, "Deployment")
	storageNodes := objectsByKind(objects, "StorageNode")
	if len(deployments) != 2 {
		t.Fatalf("Deployment count = %d, want 2", len(deployments))
	}
	if len(storageNodes) != 2 {
		t.Fatalf("StorageNode count = %d, want 2", len(storageNodes))
	}

	seenNames := map[string]bool{}
	for _, deployment := range deployments {
		name := objectName(deployment)
		if !strings.HasPrefix(name, "zfs-csi-storage-") || len(name) > 63 {
			t.Fatalf("storage Deployment name %q is not bounded and deterministic", name)
		}
		seenNames[name] = true
		spec := deployment["spec"].(map[string]any)
		if spec["replicas"] != 1 {
			t.Fatalf("%s replicas = %v, want 1", name, spec["replicas"])
		}
		strategy := spec["strategy"].(map[string]any)
		if strategy["type"] != "Recreate" {
			t.Fatalf("%s strategy = %v, want Recreate", name, strategy["type"])
		}

		text := marshalObject(t, deployment)
		switch {
		case strings.Contains(text, "--expected-owner=storage-a"):
			for _, want := range []string{"1111", "10.0.0.11", "fabric-a", "/tank-a"} {
				if !strings.Contains(text, want) {
					t.Fatalf("owner A Deployment missing %q", want)
				}
			}
			for _, leak := range []string{"2222", "10.0.0.22", "fabric-b", "/tank-b"} {
				if strings.Contains(text, leak) {
					t.Fatalf("owner A Deployment leaked owner B value %q", leak)
				}
			}
		case strings.Contains(text, "--expected-owner=storage-b"):
			for _, want := range []string{"2222", "10.0.0.22", "fabric-b", "/tank-b"} {
				if !strings.Contains(text, want) {
					t.Fatalf("owner B Deployment missing %q", want)
				}
			}
			for _, leak := range []string{"1111", "10.0.0.11", "fabric-a", "/tank-a"} {
				if strings.Contains(text, leak) {
					t.Fatalf("owner B Deployment leaked owner A value %q", leak)
				}
			}
		default:
			t.Fatalf("Deployment %q has no expected owner guard", name)
		}
	}
	if len(seenNames) != 2 {
		t.Fatalf("storage Deployment names are not unique: %v", seenNames)
	}

	for _, storageNode := range storageNodes {
		metadata := storageNode["metadata"].(map[string]any)
		annotations := metadata["annotations"].(map[string]any)
		if annotations["helm.sh/resource-policy"] != "keep" {
			t.Fatalf("StorageNode %q is not kept", objectName(storageNode))
		}
		if _, found := storageNode["status"]; found {
			t.Fatalf("StorageNode %q renders agent-owned status", objectName(storageNode))
		}
	}
}

func TestMultiOwnerSharedNetworkDomainRendersIsolatedOwners(t *testing.T) {
	owners := `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "shared", []string{"shared"}) + `,` + ownerJSON("storage-b", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "shared", []string{"shared"}) + `]`
	output := renderChart(t, "--set", "network.tls.enabled=false", "--set", "storage.enabled=true", "--set-json", "storageOwners="+owners)
	objects := renderedObjects(t, output)
	if got := len(objectsByKind(objects, "Deployment")); got != 2 {
		t.Fatalf("Deployment count = %d, want 2", got)
	}
	if got := len(objectsByKind(objects, "StorageNode")); got != 2 {
		t.Fatalf("StorageNode count = %d, want 2", got)
	}
	for _, want := range []string{"--expected-owner=storage-a", "--expected-owner=storage-b", "1111", "2222", "10.0.0.11", "10.0.0.22", "networkDomain: \"shared\""} {
		if !strings.Contains(output, want) {
			t.Fatalf("shared-domain render missing %q", want)
		}
	}
}

func TestDisabledStorageOwnerRendersNothing(t *testing.T) {
	output := renderChart(t, multiOwnerArgs(true, true)...)
	if strings.Contains(output, "--expected-owner=storage-b") || strings.Contains(output, "name: storage-b\n") {
		t.Fatal("disabled owner rendered a workload or StorageNode")
	}
	if !strings.Contains(output, "--expected-owner=storage-a") {
		t.Fatal("enabled owner was omitted")
	}
}

func TestMultiOwnerTLSMountsOnlyOwnerLeaf(t *testing.T) {
	args := append(multiOwnerArgs(true, false), "--set", "network.tls.enabled=true", "--set", "node.enabled=true")
	for _, deployment := range objectsByKind(renderedObjects(t, renderChart(t, args...)), "Deployment") {
		manifest := marshalObject(t, deployment)
		owner, ok := deployment["metadata"].(map[string]any)["labels"].(map[string]any)["zfs.csi.randomvariable.co.uk/owner"].(string)
		if !ok {
			continue
		}
		ownLeaf := "zfs-csi-tls-server-" + owner
		if !strings.Contains(manifest, ownLeaf) {
			t.Fatalf("owner %q deployment missing %q", owner, ownLeaf)
		}
		for _, other := range []string{"storage-a", "storage-b"} {
			if other != owner && strings.Contains(manifest, "zfs-csi-tls-server-"+other) {
				t.Fatalf("owner %q deployment mounts %q leaf", owner, other)
			}
		}
	}
}

func TestDisabledOwnerDoesNotWidenTLSLeafMounts(t *testing.T) {
	args := append(multiOwnerArgs(true, true), "--set", "network.tls.enabled=true", "--set", "node.enabled=true")
	output := renderChart(t, args...)
	if strings.Contains(output, "zfs-csi-tls-server-storage-b") {
		t.Fatal("disabled owner leaf rendered")
	}
	if !strings.Contains(output, "zfs-csi-tls-server-storage-a") {
		t.Fatal("enabled owner leaf missing")
	}
}

func TestTLSAuthorityLifecyclePrecedesStorageReadiness(t *testing.T) {
	args := append(multiOwnerArgs(true, false), "--set", "network.tls.enabled=true", "--set", "node.enabled=true")
	output := renderChart(t, args...)
	for _, want := range []string{
		"kind: StatefulSet", "name: zfs-csi-tls-signer", "--mode=tls-signer",
		"--tls-server-leaves=storage-a=10.0.0.11,storage-b=10.0.0.22",
		"zfs-csi-tls-server-storage-a", "zfs-csi-tls-server-storage-b",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("TLS authority render missing %q", want)
		}
	}
	if strings.Contains(output, "storage-off=") {
		t.Fatal("disabled owner received TLS authority leaf")
	}
	if strings.Contains(output, "kind: Job") {
		t.Fatal("fresh install must not contain a TLS authority bootstrap Job")
	}
}

func TestTLSAuthorityAbsentWhenTLSOff(t *testing.T) {
	output := renderChart(t, legacyStorageArgs()...)
	if strings.Contains(output, "zfs-csi-tls-signer") || strings.Contains(output, "tls-signing-authority") {
		t.Fatal("TLS-off render includes signer authority material")
	}
}

func TestStorageOwnerContractsFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		owners  string
		extra   []string
		wantErr string
	}{
		{
			name:    "mixed legacy identity",
			owners:  oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}),
			extra:   []string{"--set", "storageNode.name=legacy"},
			wantErr: "storageOwners cannot be combined with legacy storageNode.name",
		},
		{
			name:    "duplicate names",
			owners:  `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-a", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}) + `]`,
			wantErr: "duplicate storage owner name",
		},
		{
			name:    "duplicate GUID across owners",
			owners:  `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-b", "1111", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}) + `]`,
			wantErr: "authoritative pool GUID",
		},
		{
			name:    "duplicate selector across owners",
			owners:  `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + strings.Replace(ownerJSON("storage-b", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}), `"zfs.csi.randomvariable.co.uk/storage-owner":"storage-b"`, `"zfs.csi.randomvariable.co.uk/storage-owner":"storage-a"`, 1) + `]`,
			wantErr: "node selector",
		},
		{
			name:    "network domain unreachable",
			owners:  oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-b"}),
			wantErr: "reachableFrom must include networkDomain",
		},
		{
			name:    "unsafe NFS endpoint collision",
			owners:  `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-b", "2222", "10.0.0.11", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}) + `]`,
			wantErr: "unsafe endpoint collision",
		},
		{
			name:    "unsafe NVMe endpoint collision",
			owners:  `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-b", "2222", "10.0.0.22", 2049, "10.0.0.11", 4420, "fabric-b", []string{"fabric-b"}) + `]`,
			wantErr: "unsafe endpoint collision",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--set", "storage.enabled=true", "--set-json", "storageOwners=" + tc.owners}
			args = append(args, tc.extra...)
			assertRenderFails(t, args, tc.wantErr)
		})
	}
}

func TestStorageOwnerValidationRunsWithoutWorkloads(t *testing.T) {
	owners := `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-a", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}) + `]`
	assertRenderFails(t, []string{"--set-json", "storageOwners=" + owners}, "duplicate storage owner name")
}

func TestMultiPoolOwnerNFSValidation(t *testing.T) {
	multiPoolOwner := strings.Replace(
		oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}),
		`"authoritativePoolGUIDs":["1111"]`,
		`"authoritativePoolGUIDs":["1111","2222"]`,
		1,
	)
	wantErr := `storage owner "storage-a" configures multiple authoritative pools while an NFS filesystem StorageClass is enabled`

	for _, tc := range []struct {
		name  string
		extra []string
	}{
		{name: "plaintext tank", extra: []string{"--set", "storageClasses.tankNFS.enabled=true"}},
		{name: "TLS tank default", extra: nil},
		{name: "plaintext flash", extra: []string{"--set", "storageClasses.tankNFSTLS.enabled=false", "--set", "storageClasses.flashNFS.enabled=true"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--set-json", "storageOwners=" + multiPoolOwner}
			args = append(args, tc.extra...)
			assertRenderFails(t, args, wantErr)
		})
	}

	renderChart(t,
		"--set", "storage.enabled=true",
		"--set", "network.tls.enabled=false",
		"--set", "storageClasses.tankNFSTLS.enabled=false",
		"--set-json", "storageOwners="+multiPoolOwner,
	)

	disabledOwner := strings.Replace(multiPoolOwner, `"name":"storage-a"`, `"name":"storage-a","enabled":false`, 1)
	renderChart(t, "--set-json", "storageOwners="+disabledOwner)
}

func TestLegacyMultiPoolOwnerNFSValidation(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "storageNode.name=storage-a",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1111",
		"--set-string", "storageNode.authoritativePoolGUIDs[1]=2222",
	}, "legacy storage owner storageNode configures multiple authoritative pools while an NFS filesystem StorageClass is enabled")

	renderChart(t,
		"--set", "storage.enabled=true",
		"--set", "network.tls.enabled=false",
		"--set", "storageClasses.tankNFSTLS.enabled=false",
		"--set", "storageNode.name=storage-a",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1111",
		"--set-string", "storageNode.authoritativePoolGUIDs[1]=2222",
		"--set", "network.portalHost=10.0.0.11",
		"--set", "network.nfsServer=10.0.0.11",
	)
}

func TestDisabledStorageOwnerDoesNotClaimNetworkDomain(t *testing.T) {
	owners := `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-b", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-a", []string{"fabric-a"}) + `]`
	owners = strings.Replace(owners, `"name":"storage-b"`, `"name":"storage-b","enabled":false`, 1)

	output := renderChart(t, "--set", "network.tls.enabled=false", "--set", "storage.enabled=true", "--set-json", "storageOwners="+owners)
	if !strings.Contains(output, "--expected-owner=storage-a") {
		t.Fatal("enabled owner was omitted when disabled owner shared its network domain")
	}
	if strings.Contains(output, "--expected-owner=storage-b") || strings.Contains(output, "name: storage-b\n") {
		t.Fatal("disabled owner rendered despite enabled-only network-domain ownership")
	}
}

func TestStorageOwnerSchemaRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		owners  string
		wantErr string
	}{
		{name: "missing NFS", owners: `[{
"name":"storage-a","nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1111"],"poolMountRoot":"/tank","nvme":{"host":"10.0.0.1","port":4420},"networkDomain":"fabric-a","reachableFrom":["fabric-a"]}]`, wantErr: "missing property 'nfs'"},
		{name: "missing NVMe", owners: `[{
"name":"storage-a","nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1111"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.1","port":2049},"networkDomain":"fabric-a","reachableFrom":["fabric-a"]}]`, wantErr: "missing property 'nvme'"},
		{name: "empty selector", owners: `[{
"name":"storage-a","nodeSelector":{},"authoritativePoolGUIDs":["1111"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.1","port":2049},"nvme":{"host":"10.0.0.1","port":4420},"networkDomain":"fabric-a","reachableFrom":["fabric-a"]}]`, wantErr: "minProperties"},
		{name: "duplicate reachable domain", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a", "fabric-a"}), wantErr: "items at 0 and 1 are equal"},
		{name: "bracketed IPv6", owners: oneOwnerJSON("storage-a", "1111", "[2001:db8::1]", 2049, "2001:db8::1", 4420, "fabric-a", []string{"fabric-a"}), wantErr: "'not' failed"},
		{name: "host with port", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.1:2049", 2049, "10.0.0.1", 4420, "fabric-a", []string{"fabric-a"}), wantErr: "'anyOf' failed"},
		{name: "invalid domain", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "bad/domain", []string{"bad/domain"}), wantErr: "does not match pattern"},
		{name: "noncanonical GUID", owners: oneOwnerJSON("storage-a", "0011", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}), wantErr: "does not match pattern"},
		{name: "non-default NFS port", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2050, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}), wantErr: "value must be 2049"},
		{name: "zero port", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 0, "fabric-a", []string{"fabric-a"}), wantErr: "minimum"},
		{name: "large port", owners: oneOwnerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 65536, "fabric-a", []string{"fabric-a"}), wantErr: "maximum"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertRenderFails(t, []string{"--set", "storage.enabled=true", "--set-json", "storageOwners=" + tc.owners}, tc.wantErr)
		})
	}
}

func TestStorageOwnerRequiresBothEndpointsRegardlessOfStorageClass(t *testing.T) {
	missingNFS := `[{
"name":"storage-a","nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1111"],"poolMountRoot":"/tank","nvme":{"host":"10.0.0.1","port":4420},"networkDomain":"fabric-a","reachableFrom":["fabric-a"]}]`
	assertRenderFails(t, []string{
		"--set", "storage.enabled=true",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set-json", "storageOwners=" + missingNFS,
	}, "missing property 'nfs'")

	missingNVMe := `[{
"name":"storage-a","nodeSelector":{"owner":"a"},"authoritativePoolGUIDs":["1111"],"poolMountRoot":"/tank","nfs":{"host":"10.0.0.1","port":2049},"networkDomain":"fabric-a","reachableFrom":["fabric-a"]}]`
	assertRenderFails(t, []string{
		"--set", "storage.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/8"]`,
		"--set-json", "storageOwners=" + missingNVMe,
	}, "missing property 'nvme'")
}

func TestMultiOwnerIPv6HostsAreBracketedOnlyAtPortalBoundary(t *testing.T) {
	owners := oneOwnerJSON("storage-a", "1111", "2001:db8::10", 2049, "2001:db8::20", 4420, "fabric-a", []string{"fabric-a"})
	output := renderChart(t, "--set", "network.tls.enabled=false", "--set", "storage.enabled=true", "--set-json", "storageOwners="+owners)
	for _, want := range []string{"--nfs-server=2001:db8::10", "--portal-host=[2001:db8::20]:4420"} {
		if !strings.Contains(output, want) {
			t.Fatalf("IPv6 owner render missing %q", want)
		}
	}
	if strings.Contains(output, "--nfs-server=[2001:db8::10]") {
		t.Fatal("chart bracketed NFS host where runtime expects a host component")
	}
}

func legacyStorageArgs() []string {
	return []string{
		"--set", "network.tls.enabled=false",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-a",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1111",
		"--set", "storageNode.networkDomain=fabric-a",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.portalHost=10.0.0.7",
	}
}

func multiOwnerArgs(storageEnabled, disableB bool) []string {
	owners := `[` + ownerJSON("storage-a", "1111", "10.0.0.11", 2049, "10.0.0.11", 4420, "fabric-a", []string{"fabric-a"}) + `,` + ownerJSON("storage-b", "2222", "10.0.0.22", 2049, "10.0.0.22", 4420, "fabric-b", []string{"fabric-b"}) + `]`
	if disableB {
		owners = strings.Replace(owners, `"name":"storage-b"`, `"name":"storage-b","enabled":false`, 1)
	}
	return []string{"--set", "network.tls.enabled=false", "--set", fmt.Sprintf("storage.enabled=%t", storageEnabled), "--set-json", "storageOwners=" + owners}
}

func oneOwnerJSON(name, guid, nfsHost string, nfsPort int, nvmeHost string, nvmePort int, domain string, reachable []string) string {
	return `[` + ownerJSON(name, guid, nfsHost, nfsPort, nvmeHost, nvmePort, domain, reachable) + `]`
}

func ownerJSON(name, guid, nfsHost string, nfsPort int, nvmeHost string, nvmePort int, domain string, reachable []string) string {
	reachableJSON := make([]string, len(reachable))
	for i := range reachable {
		reachableJSON[i] = fmt.Sprintf("%q", reachable[i])
	}
	return fmt.Sprintf(`{"name":%q,"nodeSelector":{"zfs.csi.randomvariable.co.uk/storage-owner":%q},"tolerations":[{"key":"zfs.csi.randomvariable.co.uk/storage","operator":"Equal","value":"true","effect":"NoSchedule"}],"authoritativePoolGUIDs":[%q],"poolMountRoot":%q,"nfs":{"host":%q,"port":%d},"nvme":{"host":%q,"port":%d},"networkDomain":%q,"reachableFrom":[%s]}`,
		name, name, guid, "/tank-"+strings.TrimPrefix(name, "storage-"), nfsHost, nfsPort, nvmeHost, nvmePort, domain, strings.Join(reachableJSON, ","))
}

func renderedObjects(t *testing.T, output string) []map[string]any {
	t.Helper()
	var objects []map[string]any
	for document := range bytes.SplitSeq([]byte(output), []byte("\n---\n")) {
		var object map[string]any
		if err := yaml.Unmarshal(document, &object); err != nil {
			t.Fatalf("decode rendered manifest: %v", err)
		}
		if object["kind"] != nil {
			objects = append(objects, object)
		}
	}
	return objects
}

func objectsByKind(objects []map[string]any, kind string) []map[string]any {
	var matches []map[string]any
	for _, object := range objects {
		if object["kind"] == kind {
			matches = append(matches, object)
		}
	}
	return matches
}

func objectName(object map[string]any) string {
	metadata, _ := object["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}

func marshalObject(t *testing.T, object map[string]any) string {
	t.Helper()
	data, err := yaml.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
