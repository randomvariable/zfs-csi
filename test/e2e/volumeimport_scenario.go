// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

const (
	volumeImportCapacity  = int64(1 << 30)
	volumeImportOffset    = 16 * 1024 * 1024
	volumeImportTimeout   = 10 * time.Minute
	volumeImportBlockPath = "/dev/imported"
	volumeImportDataDir   = "consumer-data"
)

type volumeImportFixture struct {
	name    string
	backend string
	kind    zfscsiv1.VolumeType
	fsType  string
	seed    string
	write   string
	volume  *zfscsiv1.VolumeImport
}

type volumeImportNegative struct {
	name     string
	backend  string
	kind     zfscsiv1.VolumeType
	fsType   string
	capacity int64
	reason   string
}

func runVolumeImportScenario(ctx context.Context, c client.Client, kubeconfig string, node storageNode) (retErr error) {
	runID, pool := e2econfig.RunID(), e2econfig.PoolName()
	namespace := makeImportConsumerNamespace(runID)
	root := pool + "/e2e-volume-import-" + runID
	fixtures := volumeImportFixtures(pool, root, node.Name)
	negatives := volumeImportNegatives(root)

	if err := ensureImportConsumerNamespace(ctx, c, namespace); err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, cleanupVolumeImportScenario(c, node, namespace, root, fixtures, negatives))
	}()
	if err := prepareImportedBackends(ctx, c, node, fixtures, negatives); err != nil {
		return err
	}
	if err := assertVolumeImportNegatives(ctx, c, node, pool, negatives); err != nil {
		return err
	}
	for i := range fixtures {
		if err := c.Create(ctx, fixtures[i].volume); err != nil {
			return fmt.Errorf("create VolumeImport %s: %w", fixtures[i].name, err)
		}
	}
	if err := waitForVolumeImports(ctx, c, fixtures); err != nil {
		return err
	}
	if err := assertImportedVolumes(ctx, c, fixtures); err != nil {
		return err
	}
	if err := createImportedClaims(ctx, c, namespace, fixtures); err != nil {
		return err
	}
	if err := runImportedConsumers(ctx, c, namespace, fixtures, false); err != nil {
		return err
	}
	if err := deleteImportedConsumers(ctx, c, namespace, fixtures); err != nil {
		return err
	}
	if err := restartDriverWorkloads(ctx, kubeconfig); err != nil {
		return err
	}
	if err := waitForDriverReady(ctx, c); err != nil {
		return err
	}
	if err := waitForVolumeImports(ctx, c, fixtures); err != nil {
		return err
	}
	if err := assertImportedVolumes(ctx, c, fixtures); err != nil {
		return err
	}
	return runImportedConsumers(ctx, c, namespace, fixtures, true)
}

func volumeImportNegatives(root string) []volumeImportNegative {
	return []volumeImportNegative{
		{name: "backend-missing", backend: root + "-missing", kind: zfscsiv1.VolumeTypeBlock, reason: "BackendNotFound"},
		{name: "wrong-kind", backend: root + "-wrong-kind", kind: zfscsiv1.VolumeTypeFilesystem, reason: "WrongKind"},
		{name: "filesystem-small", backend: root + "-filesystem-small", kind: zfscsiv1.VolumeTypeFilesystem, capacity: 2 << 30, reason: "InsufficientCapacity"},
		{name: "zvol-small", backend: root + "-zvol-small", kind: zfscsiv1.VolumeTypeBlock, capacity: 2 << 30, reason: "InsufficientCapacity"},
		{name: "format-mismatch", backend: root + "-format-mismatch", kind: zfscsiv1.VolumeTypeBlock, fsType: "xfs", reason: "FormatMismatch"},
		{name: "export-unavailable", backend: root + "-export-unavailable", kind: zfscsiv1.VolumeTypeFilesystem, reason: "InvalidExportPath"},
		{name: "encrypted", backend: root + "-encrypted", kind: zfscsiv1.VolumeTypeFilesystem, reason: "EncryptedUnsupported"},
	}
}

func volumeImportFixtures(pool, root, owner string) []volumeImportFixture {
	fixtures := []volumeImportFixture{
		{name: "filesystem", backend: root + "-filesystem", kind: zfscsiv1.VolumeTypeFilesystem},
		{name: "block-ext4", backend: root + "-block-ext4", kind: zfscsiv1.VolumeTypeBlock, fsType: "ext4"},
		{name: "block-xfs", backend: root + "-block-xfs", kind: zfscsiv1.VolumeTypeBlock, fsType: "xfs"},
		{name: "block-raw", backend: root + "-block-raw", kind: zfscsiv1.VolumeTypeBlock},
	}
	for i := range fixtures {
		fixtures[i].seed = "volume-import-seed-" + fixtures[i].name
		fixtures[i].write = "volume-import-write-" + fixtures[i].name
		fixtures[i].volume = volumeImportObject(fixtures[i].name, pool, fixtures[i].backend, fixtures[i].kind, fixtures[i].fsType, owner)
	}
	return fixtures
}

func volumeImportObject(name, pool, backend string, kind zfscsiv1.VolumeType, fsType, owner string) *zfscsiv1.VolumeImport {
	return &zfscsiv1.VolumeImport{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-" + name, Labels: smokeOwnershipLabels()},
		Spec: zfscsiv1.VolumeImportSpec{
			Pool: pool, BackendPath: backend, Type: kind, Capacity: volumeImportCapacity,
			OwnerNode: owner, Transport: zfscsiv1.TransportNVMeTCP, FsType: fsType,
			NFSExportCIDRs: e2econfig.NFSExportCIDRs(), DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
		},
	}
}

func ensureImportConsumerNamespace(ctx context.Context, c client.Client, namespace string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: smokeOwnershipLabels()}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create import consumer namespace: %w", err)
	}
	return nil
}

func makeImportConsumerNamespace(runID string) string { return "zfs-csi-import-" + runID }

func prepareImportedBackends(ctx context.Context, c client.Client, node storageNode, fixtures []volumeImportFixture, negatives []volumeImportNegative) error {
	filesystem := fixtures[0]
	commands := filesystemImportPrepareCommands(filesystem)
	for _, fixture := range fixtures[1:] {
		device := "/dev/zvol/" + fixture.backend
		commands = append(commands,
			"zfs create -V 1G "+shellQuote(fixture.backend),
			"for i in $(seq 1 60); do test -b "+shellQuote(device)+" && break; sleep 1; done; test -b "+shellQuote(device),
		)
		switch fixture.fsType {
		case "ext4":
			commands = append(commands, "mkfs.ext4 -F "+shellQuote(device)+" >/dev/null")
		case "xfs":
			commands = append(commands, "mkfs.xfs -f "+shellQuote(device)+" >/dev/null")
		}
		if fixture.fsType == "" {
			// Raw zvol consumers use direct block I/O at a bounded non-metadata offset.
			commands = append(commands, writeBlockMarkerCommand(device, fixture.seed))
			continue
		}
		mountPath := "/mnt/zfs-csi-volume-import-" + fixture.name
		commands = append(commands, hostMountedZvolCommand(device, mountPath, fixture.fsType,
			"printf %s "+shellQuote(fixture.seed)+" > "+shellQuote(mountPath+"/original-marker")+"; sync"))
	}
	neg := map[string]volumeImportNegative{}
	for _, negative := range negatives {
		neg[negative.name] = negative
	}
	commands = append(commands,
		"zfs create -V 1G "+shellQuote(neg["wrong-kind"].backend),
		"for i in $(seq 1 60); do test -b "+shellQuote("/dev/zvol/"+neg["wrong-kind"].backend)+" && break; sleep 1; done; test -b "+shellQuote("/dev/zvol/"+neg["wrong-kind"].backend),
		writeBlockMarkerCommand("/dev/zvol/"+neg["wrong-kind"].backend, "negative-wrong-kind-marker"),
		"zfs create -p -o mountpoint="+shellQuote("/"+neg["filesystem-small"].backend)+" -o refquota=1G "+shellQuote(neg["filesystem-small"].backend),
		"printf %s "+shellQuote("negative-filesystem-marker")+" > "+shellQuote("/"+neg["filesystem-small"].backend+"/original-marker"),
		"zfs create -V 1G "+shellQuote(neg["zvol-small"].backend),
		"for i in $(seq 1 60); do test -b "+shellQuote("/dev/zvol/"+neg["zvol-small"].backend)+" && break; sleep 1; done; test -b "+shellQuote("/dev/zvol/"+neg["zvol-small"].backend),
		writeBlockMarkerCommand("/dev/zvol/"+neg["zvol-small"].backend, "negative-zvol-marker"),
		"zfs create -V 1G "+shellQuote(neg["format-mismatch"].backend),
		"for i in $(seq 1 60); do test -b "+shellQuote("/dev/zvol/"+neg["format-mismatch"].backend)+" && break; sleep 1; done; test -b "+shellQuote("/dev/zvol/"+neg["format-mismatch"].backend),
		"mkfs.ext4 -F "+shellQuote("/dev/zvol/"+neg["format-mismatch"].backend)+" >/dev/null",
		hostMountedZvolCommand("/dev/zvol/"+neg["format-mismatch"].backend, "/mnt/zfs-csi-volume-import-format-mismatch", "ext4", "printf %s "+shellQuote("negative-format-marker")+" > "+shellQuote("/mnt/zfs-csi-volume-import-format-mismatch/original-marker")+"; sync"),
		"zfs create -p -o mountpoint=none -o refquota=1G "+shellQuote(neg["export-unavailable"].backend),
		"printf %s "+shellQuote("zfs-csi-import-key")+" > "+shellQuote("/tmp/"+neg["encrypted"].name+".key"),
		"zfs create -p -o encryption=on -o keyformat=passphrase -o keylocation=file:///tmp/"+neg["encrypted"].name+".key "+shellQuote(neg["encrypted"].backend),
	)
	return runHostZFSCommand(ctx, c, "prepare", node, strings.Join(commands, "; "))
}

func filesystemImportPrepareCommands(fixture volumeImportFixture) []string {
	root := "/" + fixture.backend
	data := root + "/" + volumeImportDataDir
	return []string{
		"zfs create -p -o mountpoint=" + shellQuote(root) + " -o refquota=1G " + shellQuote(fixture.backend),
		"install -d -m 1777 " + shellQuote(data),
		"test \"$(stat -c %a " + shellQuote(root) + ")\" = 755",
		"test \"$(stat -c %a " + shellQuote(data) + ")\" = 1777",
		"printf %s " + shellQuote(fixture.seed) + " > " + shellQuote(data+"/original-marker"),
	}
}

func hostZFSCommandPod(name string, node storageNode, command string) *corev1.Pod {
	// ZFS and zvol device discovery must execute in the host mount namespace: the
	// container mount namespace has neither host udev's /dev updates nor host /sys.
	// Run one host shell rather than prefixing each fragment: compound shell
	// commands (notably the zvol-device wait loop) must retain their syntax.
	script := "nsenter -t 1 -m -u -i -n sh -ceu " + shellQuote("modprobe zfs; "+command)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "zfs-csi-volume-import-" + name + "-", Namespace: "default", Labels: smokeOwnershipLabels()},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, NodeName: node.Name, HostPID: true,
			Tolerations: []corev1.Toleration{storageNodeToleration()},
			Volumes: []corev1.Volume{
				{Name: "dev", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev"}}},
				{Name: "sys", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys"}}},
				{Name: "modules", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/lib/modules"}}},
			},
			Containers: []corev1.Container{{Name: name, Image: preflightImageFromEnv(), Command: []string{"sh", "-ceu", script},
				SecurityContext: &corev1.SecurityContext{Privileged: boolPtr(true)},
				// Surface the failing shell fragment in the terminated message so
				// waitForPodSucceeded errors name the command that actually failed
				// instead of a bare "pod failed".
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
				VolumeMounts:             []corev1.VolumeMount{{Name: "dev", MountPath: "/dev"}, {Name: "sys", MountPath: "/sys"}, {Name: "modules", MountPath: "/lib/modules", ReadOnly: true}},
			}},
		},
	}
}

func runHostZFSCommand(ctx context.Context, c client.Client, name string, node storageNode, command string) error {
	pod := hostZFSCommandPod(name, node, command)
	if err := c.Create(ctx, pod); err != nil {
		return err
	}
	runErr := waitForPodSucceeded(ctx, c, keyOf(pod), 5*time.Minute)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cleanupErr := deleteIfExists(cleanupCtx, c, pod)
	if cleanupErr == nil {
		cleanupErr = waitForScenarioObjectsGone(cleanupCtx, c, []client.Object{pod}, 90*time.Second)
	}
	return errors.Join(runErr, cleanupErr)
}

func hostMountedZvolCommand(device, mountPath, fsType, body string) string {
	mount := "mount " + shellQuote(device) + " " + shellQuote(mountPath)
	if fsType == "xfs" {
		mount = "mount -o nouuid " + shellQuote(device) + " " + shellQuote(mountPath)
	}
	return "( mkdir -p " + shellQuote(mountPath) + "; " + mount + "; cleanup() { status=$?; trap - EXIT; umount " + shellQuote(mountPath) + " || true; rmdir " + shellQuote(mountPath) + " || true; exit $status; }; trap cleanup EXIT; " + body + " )"
}

func waitForVolumeImports(ctx context.Context, c client.Client, fixtures []volumeImportFixture) error {
	return pollScenario(ctx, volumeImportTimeout, func() (bool, error) {
		for i := range fixtures {
			imp := &zfscsiv1.VolumeImport{}
			if err := c.Get(ctx, keyOf(fixtures[i].volume), imp); err != nil {
				return false, err
			}
			if imp.Status.State != zfscsiv1.VolumeImportStateReady || imp.Status.ObservedGeneration != imp.Generation {
				return false, nil
			}
		}
		return true, nil
	})
}

func assertImportedVolumes(ctx context.Context, c client.Client, fixtures []volumeImportFixture) error {
	for _, fixture := range fixtures {
		imp := &zfscsiv1.VolumeImport{}
		if err := c.Get(ctx, keyOf(fixture.volume), imp); err != nil {
			return err
		}
		if imp.Status.VolumeRef == "" || imp.Status.VolumeHandle == "" || imp.Status.ActualCapacity != volumeImportCapacity {
			return fmt.Errorf("VolumeImport %s has incomplete materialized status", imp.Name)
		}
		wantRef := naming.ImportID(imp.Spec.BackendPath)
		if imp.Status.VolumeRef != wantRef || imp.Status.VolumeRef == imp.Name {
			return fmt.Errorf("VolumeImport %s materialized identity %q, want %q", imp.Name, imp.Status.VolumeRef, wantRef)
		}
		volume := &zfscsiv1.Volume{}
		if err := c.Get(ctx, types.NamespacedName{Name: imp.Status.VolumeRef}, volume); err != nil {
			return err
		}
		if volume.Spec.Provenance != zfscsiv1.VolumeProvenanceImported || volume.Spec.BackendPath != imp.Spec.BackendPath ||
			volume.Spec.DeletionPolicy != zfscsiv1.VolumeDeletionPolicyRetain || volume.Spec.ImportFsTypeDeclaration != fixture.fsType {
			return fmt.Errorf("Volume %s does not preserve import intent", volume.Name)
		}
		if fixture.kind == zfscsiv1.VolumeTypeFilesystem && (fixture.fsType != "" || imp.Status.ExportPath == "" || volume.Status.ExportPath != imp.Status.ExportPath) {
			return fmt.Errorf("filesystem import %s has invalid authoritative export state", imp.Name)
		}
	}
	return nil
}

func assertVolumeImportNegatives(ctx context.Context, c client.Client, node storageNode, pool string, negatives []volumeImportNegative) error {
	for _, negative := range negatives {
		capacity := negative.capacity
		if capacity == 0 {
			capacity = volumeImportCapacity
		}
		imp := volumeImportObject(negative.name, pool, negative.backend, negative.kind, negative.fsType, node.Name)
		imp.Spec.Capacity = capacity
		if err := c.Create(ctx, imp); err != nil {
			return fmt.Errorf("create negative VolumeImport %s: %w", negative.name, err)
		}
		if err := waitForFailedVolumeImport(ctx, c, imp, negative.reason); err != nil {
			return err
		}
		if err := assertNoMaterializedVolume(ctx, c, imp); err != nil {
			return err
		}
		if err := assertNegativeBackendUnchanged(ctx, c, node, negative); err != nil {
			return err
		}
	}
	return nil
}

func waitForFailedVolumeImport(ctx context.Context, c client.Client, imp *zfscsiv1.VolumeImport, wantReason string) error {
	return pollScenario(ctx, 3*time.Minute, func() (bool, error) {
		current := &zfscsiv1.VolumeImport{}
		if err := c.Get(ctx, keyOf(imp), current); err != nil {
			return false, err
		}
		if current.Status.State != zfscsiv1.VolumeImportStateFailed {
			return false, nil
		}
		for _, condition := range current.Status.Conditions {
			if condition.Type == "Ready" && condition.Reason == wantReason && condition.Status == metav1.ConditionFalse {
				return true, nil
			}
		}
		return false, fmt.Errorf("VolumeImport %s failed without reason %q", current.Name, wantReason)
	})
}

func assertNoMaterializedVolume(ctx context.Context, c client.Client, imp *zfscsiv1.VolumeImport) error {
	current := &zfscsiv1.VolumeImport{}
	if err := c.Get(ctx, keyOf(imp), current); err != nil {
		return err
	}
	if current.Status.VolumeRef != "" || current.Status.VolumeHandle != "" {
		return fmt.Errorf("failed VolumeImport %s materialized ref=%q handle=%q", current.Name, current.Status.VolumeRef, current.Status.VolumeHandle)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := c.List(ctx, volumes); err != nil {
		return err
	}
	for _, volume := range volumes.Items {
		if volume.Spec.BackendPath == current.Spec.BackendPath {
			return fmt.Errorf("failed VolumeImport %s materialized Volume %s", current.Name, volume.Name)
		}
	}
	return nil
}

func assertNegativeBackendUnchanged(ctx context.Context, c client.Client, node storageNode, negative volumeImportNegative) error {
	device := "/dev/zvol/" + negative.backend
	var command string
	switch negative.name {
	case "backend-missing":
		return nil
	case "wrong-kind":
		command = "test \"$(zfs get -H -o value volsize " + shellQuote(negative.backend) + ")\" = 1G; " + readBlockMarkerCommand(device, "negative-wrong-kind-marker")
	case "filesystem-small":
		command = "test \"$(zfs get -H -o value refquota " + shellQuote(negative.backend) + ")\" = 1G; test \"$(cat " + shellQuote("/"+negative.backend+"/original-marker") + ")\" = " + shellQuote("negative-filesystem-marker")
	case "zvol-small":
		command = "test \"$(zfs get -H -o value volsize " + shellQuote(negative.backend) + ")\" = 1G; " + readBlockMarkerCommand(device, "negative-zvol-marker")
	case "format-mismatch":
		mountPath := "/mnt/zfs-csi-volume-import-negative-format-mismatch"
		command = "test \"$(blkid -o value -s TYPE " + shellQuote(device) + ")\" = ext4; " + hostMountedZvolCommand(device, mountPath, "ext4", "test \"$(cat "+shellQuote(mountPath+"/original-marker")+")\" = "+shellQuote("negative-format-marker"))
	case "export-unavailable":
		command = "test \"$(zfs get -H -o value refquota " + shellQuote(negative.backend) + ")\" = 1G; test \"$(zfs get -H -o value mountpoint " + shellQuote(negative.backend) + ")\" = none; test \"$(zfs get -H -o value sharenfs " + shellQuote(negative.backend) + ")\" = off"
	case "encrypted":
		command = "test \"$(zfs get -H -o value encryption " + shellQuote(negative.backend) + ")\" != off; test \"$(zfs get -H -o value sharenfs " + shellQuote(negative.backend) + ")\" = off"
	default:
		return fmt.Errorf("unknown negative backend %q", negative.name)
	}
	return runHostZFSCommand(ctx, c, "negative-check-"+negative.name, node, command)
}

func createImportedClaims(ctx context.Context, c client.Client, namespace string, fixtures []volumeImportFixture) error {
	for _, fixture := range fixtures {
		imp := &zfscsiv1.VolumeImport{}
		if err := c.Get(ctx, keyOf(fixture.volume), imp); err != nil {
			return err
		}
		pv, pvc := importedStaticClaim(namespace, fixture, imp)
		if err := c.Create(ctx, pv); err != nil {
			return fmt.Errorf("create imported PV %s: %w", pv.Name, err)
		}
		if err := c.Create(ctx, pvc); err != nil {
			return fmt.Errorf("create imported PVC %s: %w", pvc.Name, err)
		}
		if _, err := boundVolumeIdentity(ctx, c, pvc); err != nil {
			return fmt.Errorf("bind imported PVC %s: %w", pvc.Name, err)
		}
		if err := assertImportedStaticPV(ctx, c, pv.Name, pvc, fixture, imp); err != nil {
			return err
		}
	}
	return nil
}

func importedStaticClaim(namespace string, fixture volumeImportFixture, imp *zfscsiv1.VolumeImport) (*corev1.PersistentVolume, *corev1.PersistentVolumeClaim) {
	mode, access := corev1.PersistentVolumeBlock, corev1.ReadWriteOnce
	fsType, attributes := fixture.fsType, map[string]string(nil)
	if fixture.kind == zfscsiv1.VolumeTypeFilesystem {
		mode, access, fsType = corev1.PersistentVolumeFilesystem, corev1.ReadWriteMany, ""
		attributes = map[string]string{"provenance": string(zfscsiv1.VolumeProvenanceImported), "exportPath": imp.Status.ExportPath}
	} else if fixture.fsType != "" {
		mode = corev1.PersistentVolumeFilesystem
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: imp.Name + "-pv", Labels: smokeOwnershipLabels()},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(volumeImportCapacity, resource.BinarySI)},
			AccessModes: []corev1.PersistentVolumeAccessMode{access}, VolumeMode: volumeModePtr(mode),
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, StorageClassName: "",
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: zfsCSIDriverName, VolumeHandle: imp.Status.VolumeHandle, FSType: fsType, VolumeAttributes: attributes,
			}},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: imp.Name + "-claim", Namespace: namespace, Labels: smokeOwnershipLabels()},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{access}, VolumeMode: volumeModePtr(mode), StorageClassName: stringPtr(""), VolumeName: pv.Name,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(volumeImportCapacity, resource.BinarySI)}},
		},
	}
	return pv, pvc
}

func assertImportedStaticPV(ctx context.Context, c client.Client, name string, pvc *corev1.PersistentVolumeClaim, fixture volumeImportFixture, imp *zfscsiv1.VolumeImport) error {
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, pv); err != nil {
		return err
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain || pv.Spec.StorageClassName != "" || pv.Spec.CSI == nil ||
		pv.Spec.CSI.Driver != zfsCSIDriverName || pv.Spec.CSI.VolumeHandle != imp.Status.VolumeHandle || pv.Spec.CSI.FSType != fixture.fsType ||
		pv.Spec.VolumeMode == nil || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.Capacity.Storage().CmpInt64(volumeImportCapacity) != 0 {
		return fmt.Errorf("imported PV %s lost static binding intent", name)
	}
	if fixture.kind == zfscsiv1.VolumeTypeFilesystem {
		if *pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem || pv.Spec.AccessModes[0] != corev1.ReadWriteMany || pv.Spec.CSI.VolumeAttributes["exportPath"] != imp.Status.ExportPath {
			return fmt.Errorf("filesystem PV %s has wrong NFS import attributes", name)
		}
	} else if fixture.fsType != "" && (*pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem || pv.Spec.AccessModes[0] != corev1.ReadWriteOnce) {
		return fmt.Errorf("formatted block PV %s has wrong static volume mode", name)
	} else if fixture.fsType == "" && (*pv.Spec.VolumeMode != corev1.PersistentVolumeBlock || pv.Spec.AccessModes[0] != corev1.ReadWriteOnce) {
		return fmt.Errorf("block PV %s has wrong static volume mode", name)
	}
	return nil
}

func runImportedConsumers(ctx context.Context, c client.Client, namespace string, fixtures []volumeImportFixture, afterRestart bool) error {
	for _, fixture := range fixtures {
		pod := importedConsumer(namespace, fixture, afterRestart)
		if err := c.Create(ctx, pod); err != nil {
			return fmt.Errorf("create import consumer %s: %w", fixture.name, err)
		}
	}
	for _, fixture := range fixtures {
		if err := waitForPodReady(ctx, c, types.NamespacedName{Namespace: namespace, Name: importedConsumerName(fixture)}, scenarioPodTimeout); err != nil {
			return fmt.Errorf("consumer %s did not prove imported data: %w", fixture.name, err)
		}
	}
	return nil
}

func importedConsumer(namespace string, fixture volumeImportFixture, afterRestart bool) *corev1.Pod {
	claim := fixture.volume.Name + "-claim"
	if fixture.kind == zfscsiv1.VolumeTypeFilesystem {
		markerDir := "/data/" + volumeImportDataDir
		command := "test \"$(cat " + shellQuote(markerDir+"/original-marker") + ")\" = " + shellQuote(fixture.seed) + "; "
		if afterRestart {
			command += "test \"$(cat " + shellQuote(markerDir+"/restarted-marker") + ")\" = " + shellQuote(fixture.write) + "; "
		} else {
			command += "printf %s " + shellQuote(fixture.write) + " > " + shellQuote(markerDir+"/restarted-marker") + "; sync; test \"$(cat " + shellQuote(markerDir+"/restarted-marker") + ")\" = " + shellQuote(fixture.write) + "; "
		}
		command += "touch " + scenarioReadyFile + "; exec sleep 3600"
		pod := smokePod(namespace, importedConsumerName(fixture), claim, "", command)
		pod.Spec.Containers[0].ReadinessProbe = scenarioReadinessProbe()
		pod.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
			RunAsUser:  int64p(65534),
			RunAsGroup: int64p(65534),
		}
		return pod
	}
	if fixture.fsType != "" {
		expected := fixture.seed
		if afterRestart {
			expected = fixture.write
		}
		command := "test \"$(cat /data/original-marker)\" = " + shellQuote(expected) + "; printf %s " + shellQuote(fixture.write) + " > /data/original-marker; sync; test \"$(cat /data/original-marker)\" = " + shellQuote(fixture.write) + "; touch " + scenarioReadyFile + "; exec sleep 3600"
		pod := smokePod(namespace, importedConsumerName(fixture), claim, "", command)
		pod.Spec.Containers[0].ReadinessProbe = scenarioReadinessProbe()
		pod.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		return pod
	}
	expected := fixture.seed
	if afterRestart {
		expected = fixture.write
	}
	command := readBlockMarkerCommand(volumeImportBlockPath, expected) + "; " + writeBlockMarkerCommand(volumeImportBlockPath, fixture.write) + "; " + readBlockMarkerCommand(volumeImportBlockPath, fixture.write) + "; touch " + scenarioReadyFile + "; exec sleep 3600"
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: importedConsumerName(fixture), Namespace: namespace, Labels: func() map[string]string {
			labels := smokeOwnershipLabels()
			labels["app.kubernetes.io/name"] = "zfs-csi-e2e"
			return labels
		}()},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
			// Pin to the consumer group (nodes with the CSI node plugin); an
			// unconstrained pod can land on a plugin-less node and hang mounts.
			NodeSelector: smokeConsumerNodeSelector(),
			Containers: []corev1.Container{{Name: "consumer", Image: "busybox:1.37", Command: []string{"sh", "-ceu", command},
				VolumeDevices: []corev1.VolumeDevice{{Name: "data", DevicePath: volumeImportBlockPath}}, ReadinessProbe: scenarioReadinessProbe(),
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError}},
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}}},
		},
	}
}

func importedConsumerName(fixture volumeImportFixture) string {
	return fixture.volume.Name + "-consumer"
}

func writeBlockMarkerCommand(device, marker string) string {
	return "printf %s " + shellQuote(marker) + " | dd of=" + shellQuote(device) + " bs=1 seek=" + fmt.Sprint(volumeImportOffset) + " conv=notrunc status=none"
}

func readBlockMarkerCommand(device, marker string) string {
	return "test \"$(dd if=" + shellQuote(device) + " bs=1 skip=" + fmt.Sprint(volumeImportOffset) + " count=" + fmt.Sprint(len(marker)) + " status=none)\" = " + shellQuote(marker)
}

func deleteImportedConsumers(ctx context.Context, c client.Client, namespace string, fixtures []volumeImportFixture) error {
	objects := make([]client.Object, 0, len(fixtures))
	for _, fixture := range fixtures {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: importedConsumerName(fixture), Namespace: namespace}})
	}
	for _, obj := range objects {
		if err := deleteIfExists(ctx, c, obj); err != nil {
			return err
		}
	}
	return waitForScenarioObjectsGone(ctx, c, objects, 3*time.Minute)
}

func cleanupVolumeImportScenario(c client.Client, node storageNode, namespace, root string, fixtures []volumeImportFixture, negatives []volumeImportNegative) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err := deleteImportedConsumers(ctx, c, namespace, fixtures); err != nil {
		// Consumers are the teardown safety boundary. Nothing below is safe while
		// a pod may still hold a mount or block device open.
		return err
	}
	var errs []error
	claims := make([]client.Object, 0, len(fixtures)*2)
	pvNames := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		pvc := &corev1.PersistentVolumeClaim{}
		err := c.Get(ctx, types.NamespacedName{Name: fixture.volume.Name + "-claim", Namespace: namespace}, pvc)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("capture imported PVC %s PV identity: %w", pvc.Name, err)
		}
		if err == nil && pvc.Spec.VolumeName != "" {
			pvNames = append(pvNames, pvc.Spec.VolumeName)
		} else {
			pvNames = append(pvNames, fixture.volume.Name+"-pv")
		}
		claims = append(claims,
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: fixture.volume.Name + "-claim", Namespace: namespace}},
			&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: fixture.volume.Name + "-pv"}},
		)
	}
	for _, claim := range claims {
		if err := deleteIfExists(ctx, c, claim); err != nil {
			return err
		}
	}
	if err := waitForImportClaimsAndAttachmentsGone(ctx, c, claims, pvNames); err != nil {
		return err
	}

	volumes, volumeErr := materializedImportVolumes(ctx, c, fixtures, negatives)
	errs = appendError(errs, volumeErr)
	for _, imp := range allVolumeImports(fixtures, negatives) {
		errs = appendError(errs, deleteIfExists(ctx, c, imp))
	}
	errs = appendError(errs, waitForVolumeImportObjectsGone(ctx, c, allVolumeImports(fixtures, negatives)))
	// Import deletion must not remove its retained Volume. This proves lifecycle
	// decoupling before the test explicitly unexports the materialized resources.
	if volumeErr == nil {
		errs = appendError(errs, assertMaterializedVolumesRemain(ctx, c, volumes))
		errs = appendError(errs, assertRetainedBackends(ctx, c, node, fixtures))
	}
	for _, volume := range volumes {
		if err := deleteIfExists(ctx, c, volume); err != nil {
			return errors.Join(append(errs, err)...)
		}
	}
	if len(volumes) != 0 {
		if err := waitForScenarioObjectsGone(ctx, c, volumeObjects(volumes), 5*time.Minute); err != nil {
			return errors.Join(append(errs, err)...)
		}
		// Retain finalizers must not destroy or mutate external backends.
		errs = appendError(errs, assertRetainedBackends(ctx, c, node, fixtures))
	}
	errs = appendError(errs, deleteIfExists(ctx, c, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	errs = appendError(errs, destroyImportedBackends(ctx, c, node, root, negatives))
	return errors.Join(errs...)
}

func waitForImportClaimsAndAttachmentsGone(ctx context.Context, c client.Client, claims []client.Object, pvNames []string) error {
	wanted := make(map[string]struct{}, len(pvNames))
	for _, name := range pvNames {
		wanted[name] = struct{}{}
	}
	return pollScenario(ctx, 5*time.Minute, func() (bool, error) {
		for _, claim := range claims {
			if err := c.Get(ctx, keyOf(claim), claim.DeepCopyObject().(client.Object)); !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		attachments := &storagev1.VolumeAttachmentList{}
		if err := c.List(ctx, attachments); err != nil {
			return false, err
		}
		for i := range attachments.Items {
			pvName := attachments.Items[i].Spec.Source.PersistentVolumeName
			if pvName != nil {
				if _, ok := wanted[*pvName]; ok {
					return false, nil
				}
			}
		}
		return true, nil
	})
}

func appendError(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}

func allVolumeImports(fixtures []volumeImportFixture, negatives []volumeImportNegative) []client.Object {
	imports := make([]client.Object, 0, len(fixtures)+len(negatives))
	for _, fixture := range fixtures {
		imports = append(imports, fixture.volume)
	}
	for _, negative := range negatives {
		imports = append(imports, &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "e2e-" + negative.name, Labels: smokeOwnershipLabels()}})
	}
	return imports
}

func materializedImportVolumes(ctx context.Context, c client.Client, fixtures []volumeImportFixture, negatives []volumeImportNegative) ([]*zfscsiv1.Volume, error) {
	volumes := make([]*zfscsiv1.Volume, 0, len(fixtures))
	var errs []error
	for _, imp := range allVolumeImports(fixtures, negatives) {
		current := &zfscsiv1.VolumeImport{}
		if err := c.Get(ctx, keyOf(imp), current); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("get VolumeImport %s: %w", imp.GetName(), err))
			continue
		}
		if current.Status.VolumeRef == "" {
			continue
		}
		volumes = append(volumes, &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: current.Status.VolumeRef}})
	}
	return volumes, errors.Join(errs...)
}

func assertMaterializedVolumesRemain(ctx context.Context, c client.Client, volumes []*zfscsiv1.Volume) error {
	for _, volume := range volumes {
		if err := c.Get(ctx, keyOf(volume), &zfscsiv1.Volume{}); err != nil {
			return fmt.Errorf("materialized Volume %s disappeared after VolumeImport deletion: %w", volume.Name, err)
		}
	}
	return nil
}

func volumeObjects(volumes []*zfscsiv1.Volume) []client.Object {
	objects := make([]client.Object, 0, len(volumes))
	for _, volume := range volumes {
		objects = append(objects, volume)
	}
	return objects
}

func waitForVolumeImportObjectsGone(ctx context.Context, c client.Client, imports []client.Object) error {
	return pollScenario(ctx, 5*time.Minute, func() (bool, error) {
		for _, imp := range imports {
			if err := c.Get(ctx, keyOf(imp), &zfscsiv1.VolumeImport{}); !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		return true, nil
	})
}

func assertRetainedBackends(ctx context.Context, c client.Client, node storageNode, fixtures []volumeImportFixture) error {
	commands := make([]string, 0, len(fixtures)*2)
	for _, fixture := range fixtures {
		if fixture.kind == zfscsiv1.VolumeTypeFilesystem {
			commands = append(commands, retainedFilesystemBackendChecks(fixture)...)
			continue
		}
		if fixture.fsType != "" {
			mountPath := "/mnt/zfs-csi-volume-import-" + fixture.name
			commands = append(commands, hostMountedZvolCommand("/dev/zvol/"+fixture.backend, mountPath, fixture.fsType,
				"test \"$(cat "+shellQuote(mountPath+"/original-marker")+")\" = "+shellQuote(fixture.write)))
			continue
		}
		device := "/dev/zvol/" + fixture.backend
		commands = append(commands,
			"test \"$(zfs get -H -o value volsize "+shellQuote(fixture.backend)+")\" = 1G",
			readBlockMarkerCommand(device, fixture.write),
		)
	}
	return runHostZFSCommand(ctx, c, "retain-check", node, strings.Join(commands, "; "))
}

func retainedFilesystemBackendChecks(fixture volumeImportFixture) []string {
	root := "/" + fixture.backend
	data := root + "/" + volumeImportDataDir
	return []string{
		"test \"$(zfs get -H -o value mounted " + shellQuote(fixture.backend) + ")\" = yes",
		"test \"$(stat -c %a " + shellQuote(root) + ")\" = 755",
		"test \"$(stat -c %a " + shellQuote(data) + ")\" = 1777",
		"test \"$(cat " + shellQuote(data+"/original-marker") + ")\" = " + shellQuote(fixture.seed),
		"test \"$(cat " + shellQuote(data+"/restarted-marker") + ")\" = " + shellQuote(fixture.write),
		"test \"$(zfs get -H -o value refquota " + shellQuote(fixture.backend) + ")\" = 1G",
	}
}

func destroyImportedBackends(ctx context.Context, c client.Client, node storageNode, root string, negatives []volumeImportNegative) error {
	filesystem := root + "-filesystem"
	paths := []string{filesystem, root + "-block-ext4", root + "-block-xfs", root + "-block-raw"}
	for _, negative := range negatives {
		paths = append(paths, negative.backend)
	}
	var errs []error
	for _, path := range paths {
		command := destroyImportedBackendCommand(path, path == filesystem)
		errs = appendError(errs, runHostZFSCommand(ctx, c, "destroy", node, command))
	}
	// Key-file removal is independent from the encrypted dataset teardown.
	errs = appendError(errs, runHostZFSCommand(ctx, c, "remove-key", node, "rm -f /tmp/encrypted.key"))
	return errors.Join(errs...)
}

func destroyImportedBackendCommand(path string, filesystem bool) string {
	return destroyImportedBackendCommandWithUnmountRetry(path, filesystem, 24, 10)
}

func destroyImportedBackendCommandWithUnmountRetry(path string, filesystem bool, attempts, intervalSeconds int) string {
	command := "if zfs list -H " + shellQuote(path) + " >/dev/null 2>&1; then "
	if filesystem {
		mountpoint := "/" + path
		command += "test \"$(zfs get -H -o value mountpoint " + shellQuote(path) + ")\" = " + shellQuote(mountpoint) + "; " +
			fmt.Sprintf("unmount_attempt=1; while [ \"$(zfs get -H -o value mounted %s)\" = yes ]; do "+
				"if unmount_output=$(zfs unmount %s 2>&1); then break; else unmount_status=$?; fi; "+
				"printf 'zfs unmount %%s attempt %%s/%d failed (status %%s): %%s\\n' %s \"$unmount_attempt\" \"$unmount_status\" \"$unmount_output\" >&2; "+
				"case $unmount_output in *' is busy'*) ;; *) exit \"$unmount_status\" ;; esac; "+
				"[ \"$unmount_attempt\" -lt %d ] || exit \"$unmount_status\"; "+
				"unmount_attempt=$((unmount_attempt + 1)); sleep %d; done; ", shellQuote(path), shellQuote(path), attempts, shellQuote(path), attempts, intervalSeconds) +
			"scan_mountinfo() { for mountinfo in /proc/[0-9]*/mountinfo; do " +
			"[ -e \"$mountinfo\" ] || continue; " +
			"if awk -v want_mount=" + shellQuote(mountpoint) + " -v want_source=" + shellQuote(path) + " '" +
			"{ sep=0; for (i=7; i<=NF; i++) if ($i == \"-\") { sep=i; break } " +
			"if (NF < 10 || sep == 0 || sep + 2 > NF) exit 20; " +
			"if ($5 == want_mount || ($(sep+1) == \"zfs\" && $(sep+2) == want_source)) exit 42 }' \"$mountinfo\"; then status=0; else status=$?; fi; " +
			"case $status in 0) ;; 42) return 1 ;; *) [ ! -e \"$mountinfo\" ] || return 2 ;; esac; " +
			"done; return 0; }; scan_mountinfo; sleep 1; scan_mountinfo; "
	}
	return command + "zfs destroy -r " + shellQuote(path) + "; fi"
}

func restartDriverWorkloads(ctx context.Context, kubeconfig string) error {
	targets, err := activeDriverRolloutTargets(ctx, kubeconfig)
	if err != nil {
		return fmt.Errorf("discover driver rollout targets: %w", err)
	}
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "restart", target.resource, "-n", zfsCSINamespace).CombinedOutput()
		if err != nil {
			return fmt.Errorf("restart %s: %w: %s", target.resource, err, out)
		}
	}
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "status", target.resource, "-n", zfsCSINamespace, "--timeout", "5m").CombinedOutput()
		if err != nil {
			return fmt.Errorf("wait for restarted %s: %w: %s", target.resource, err, out)
		}
	}
	return nil
}

func volumeModePtr(mode corev1.PersistentVolumeMode) *corev1.PersistentVolumeMode { return &mode }

func int64p(value int64) *int64 { return &value }

// Encrypted backend imports are deliberately excluded: the AWS E2E lane has no
// external key lifecycle fixture. Production rejection remains covered by
// internal/agent/volumeimport_reconciler_test.go until this harness can supply keys.
