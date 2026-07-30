// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package zfscsi

import (
	"strings"
	"testing"
)

func TestNodeLabelModeOmitsGlobalEndpointFallbacks(t *testing.T) {
	output := renderChart(t,
		"--set", "network.tls.enabled=false",
		"--set", "node.enabled=true",
		"--set", "node.networkDomainSource=nodeLabel",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
	)
	for _, unwanted := range []string{"--portal-host=", "--nfs-server=", "--network-domain="} {
		if strings.Contains(output, unwanted) {
			t.Fatalf("node-label render contains legacy fallback %q", unwanted)
		}
	}
	if strings.Count(output, "kind: DaemonSet") != 1 || strings.Count(output, "name: zfs-csi-node") < 1 {
		t.Fatal("node-label mode must retain one node DaemonSet")
	}
	for _, want := range []string{
		"--network-domain-source=nodeLabel",
		"--network-domain-label=topology.zfs.csi.randomvariable.co.uk/network-domain",
		"fieldPath: spec.nodeName",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("node-label render missing %q", want)
		}
	}
}

func TestNodeNVMeModuleLoaderIsGatedAndMountsHostKernel(t *testing.T) {
	base := []string{"--set", "network.tls.enabled=false", "--set", "node.enabled=true", "--set", "node.networkDomain=workers", "--set", "network.nfsServer=10.0.0.7"}
	node := nodeDaemonSetDocument(t, renderChart(t, append(base, "--set", "storageClasses.tankNVMe.enabled=true", "--set", "network.portalHost=10.0.0.7")...))
	for _, want := range []string{"name: load-nvme-modules", "nvme-fabrics", "nvme-tcp", "mountPath: /lib/modules", "readOnly: true", "path: /lib/modules", "mountPath: /sys", "privileged: true"} {
		if !strings.Contains(node, want) {
			t.Fatalf("NVMe node render missing %q:\n%s", want, node)
		}
	}
	nfsOnly := nodeDaemonSetDocument(t, renderChart(t, append(base,
		"--set", "storageClasses.tankNVMe.enabled=false",
		"--set", "storageClasses.tankNVMeTLS.enabled=false",
		"--set", "storageClasses.flashNVMe.enabled=false",
	)...))
	if strings.Contains(nfsOnly, "load-nvme-modules") {
		t.Fatal("NFS-only node render must not require NVMe module loading")
	}
}

func TestStaticNodeDomainRetainsLegacyFallbacks(t *testing.T) {
	output := renderChart(t,
		"--set", "network.tls.enabled=false",
		"--set", "node.enabled=true",
		"--set", "node.networkDomain=workers",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
	)
	for _, want := range []string{"--network-domain=workers", "--portal-host=10.0.0.7", "--nfs-server=10.0.0.7"} {
		if !strings.Contains(output, want) {
			t.Fatalf("static render missing %q", want)
		}
	}
}

func TestNodePreservesUserNodeSelector(t *testing.T) {
	baseArgs := []string{
		"--set", "network.tls.enabled=false",
		"--set", "node.enabled=true",
		"--set", "node.networkDomain=workers",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
	}

	t.Run("default runs on every node", func(t *testing.T) {
		output := renderChart(t, baseArgs...)
		node := nodeDaemonSetDocument(t, output)
		if strings.Contains(node, "nodeSelector:") {
			t.Fatalf("node DaemonSet unexpectedly has a default nodeSelector:\n%s", node)
		}
	})

	t.Run("user selector is preserved", func(t *testing.T) {
		args := append(append([]string{}, baseArgs...),
			"--set-string", `node.nodeSelector.consumer-group=storage`,
		)
		output := renderChart(t, args...)
		node := nodeDaemonSetDocument(t, output)
		if !strings.Contains(node, "consumer-group: storage") {
			t.Fatalf("node DaemonSet missing configured nodeSelector:\n%s", node)
		}
	})
}

func TestNVMetControllerUsesAMD64StorageSelector(t *testing.T) {
	output := renderChart(t,
		"--set", "network.tls.enabled=false",
		"--set", "nvmet.enabled=true",
		"--set", "storageNode.name=storage-a",
		"--set", "network.portalHost=10.0.0.7",
		"--set-string", "storageNode.selector.consumer-group=storage",
	)
	controllers := objectsByKind(renderedObjects(t, output), "DaemonSet")
	if len(controllers) != 1 || objectName(controllers[0]) != "nvmet-controller" {
		t.Fatalf("nvmet DaemonSets = %#v, want one nvmet-controller", controllers)
	}
	assertNodeSelectorValues(t, controllers[0], map[string]string{
		"kubernetes.io/arch":                   "amd64",
		"zfs.csi.randomvariable.co.uk/storage": "true",
		"consumer-group":                       "storage",
	})
	controller := marshalObject(t, controllers[0])
	for _, want := range []string{"name: load-nvmet-modules", "modprobe nvme-keyring", "modprobe nvmet", "modprobe nvmet-tcp", "mountPath: /lib/modules", "readOnly: true", "mountPath: /sys", "privileged: true"} {
		if !strings.Contains(controller, want) {
			t.Fatalf("nvmet controller render missing %q:\n%s", want, controller)
		}
	}
}

// nodeDaemonSetDocument extracts the rendered zfs-csi-node DaemonSet document
// so selector assertions cannot false-positive on other workloads' selectors.
func nodeDaemonSetDocument(t *testing.T, output string) string {
	t.Helper()
	for _, document := range strings.Split(output, "\n---\n") {
		if strings.Contains(document, "kind: DaemonSet") && strings.Contains(document, "name: zfs-csi-node") {
			return document
		}
	}
	t.Fatal("rendered chart lacks the zfs-csi-node DaemonSet")
	return ""
}

func TestMultiOwnerForcesNodeLabelModeAndOmitsOwnerEndpoints(t *testing.T) {
	args := append(multiOwnerArgs(false, false), "--set", "node.enabled=true")
	output := renderChart(t, args...)
	for _, want := range []string{
		"--network-domain-source=nodeLabel",
		"--network-domain-label=topology.zfs.csi.randomvariable.co.uk/network-domain",
		"fieldPath: spec.nodeName",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("multi-owner node render missing %q", want)
		}
	}
	for _, unwanted := range []string{"--network-domain=", "--portal-host=", "--nfs-server=", "10.0.0.11", "10.0.0.22"} {
		if strings.Contains(output, unwanted) {
			t.Fatalf("multi-owner node render leaked %q", unwanted)
		}
	}
}
