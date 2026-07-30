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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func TestVolumeImportFixturesStayOutsideManagedCSIPath(t *testing.T) {
	fixtures := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")
	for _, fixture := range fixtures {
		if fixture.volume.Spec.Pool != "testpool" || !strings.HasPrefix(fixture.backend, "testpool/") {
			t.Fatalf("fixture does not preserve requested pool: %#v", fixture)
		}
		if fixture.backend == "testpool/csi" || strings.HasPrefix(fixture.backend, "testpool/csi/") {
			t.Fatalf("fixture uses managed CSI path: %q", fixture.backend)
		}
	}
}

func TestFilesystemImportPreparationPreservesRootAndCreatesWritableDataDirectory(t *testing.T) {
	fixture := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")[0]
	commands := strings.Join(filesystemImportPrepareCommands(fixture), "; ")
	for _, want := range []string{
		"install -d -m 1777 '" + "/" + fixture.backend + "/" + volumeImportDataDir + "'",
		"stat -c %a '" + "/" + fixture.backend + "'",
		"= 755",
		"stat -c %a '" + "/" + fixture.backend + "/" + volumeImportDataDir + "'",
		"= 1777",
		"/" + fixture.backend + "/" + volumeImportDataDir + "/original-marker",
	} {
		if !strings.Contains(commands, want) {
			t.Errorf("filesystem preparation misses %q: %s", want, commands)
		}
	}
}

func TestRetainedFilesystemBackendChecksMountAndPersistedMarkers(t *testing.T) {
	fixture := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")[0]
	command := strings.Join(retainedFilesystemBackendChecks(fixture), "; ")
	for _, want := range []string{
		"zfs get -H -o value mounted '" + fixture.backend + "'",
		"= yes",
		"/" + fixture.backend + "/" + volumeImportDataDir + "/original-marker",
		"/" + fixture.backend + "/" + volumeImportDataDir + "/restarted-marker",
	} {
		if !strings.Contains(command, want) {
			t.Errorf("retained backend checks miss %q: %s", want, command)
		}
	}
}

func TestImportedStaticClaimsUseCorrectVolumeModes(t *testing.T) {
	fixtures := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")
	for _, fixture := range fixtures {
		imp := fixture.volume.DeepCopy()
		imp.Status.VolumeHandle = "csi:test"
		imp.Status.ExportPath = "/testpool/e2e-volume-import-run-filesystem"
		pv, _ := importedStaticClaim("imports", fixture, imp)
		want := corev1.PersistentVolumeBlock
		if fixture.kind == "filesystem" || fixture.fsType != "" {
			want = corev1.PersistentVolumeFilesystem
		}
		if pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != want {
			t.Fatalf("%s volume mode = %v, want %s", fixture.name, pv.Spec.VolumeMode, want)
		}
	}
}

func TestVolumeImportCleanupPlansAllKnownImports(t *testing.T) {
	fixtures := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")
	negatives := volumeImportNegatives("testpool/e2e-volume-import-run")
	imports := allVolumeImports(fixtures, negatives)
	if len(imports) != len(fixtures)+len(negatives) {
		t.Fatalf("cleanup imports = %d, want %d", len(imports), len(fixtures)+len(negatives))
	}
	for _, imp := range imports {
		if imp.GetName() == "" || imp.GetNamespace() != "" {
			t.Fatalf("invalid cluster cleanup import identity: %s/%s", imp.GetNamespace(), imp.GetName())
		}
	}
}

func TestVolumeImportFilesystemCleanupUnmountsAndChecksAllMountNamespaces(t *testing.T) {
	root := "testpool/e2e-volume-import-run"
	command := destroyImportedBackendCommand(root+"-filesystem", true)
	for _, want := range []string{
		"zfs get -H -o value mountpoint '" + root + "-filesystem'",
		"= '/" + root + "-filesystem'",
		"zfs unmount '" + root + "-filesystem'",
		"/proc/[0-9]*/mountinfo",
		"$5 == want_mount",
		"$(sep+1) == \"zfs\" && $(sep+2) == want_source",
		"exit 20",
		"[ ! -e \"$mountinfo\" ] || return 2",
		"scan_mountinfo; sleep 1; scan_mountinfo",
		"zfs destroy -r '" + root + "-filesystem'",
	} {
		if !strings.Contains(command, want) {
			t.Errorf("filesystem cleanup misses %q: %s", want, command)
		}
	}
	for _, forbidden := range []string{"zfs unmount -f", "zfs unmount -l", "umount -l"} {
		if strings.Contains(command, forbidden) {
			t.Errorf("filesystem cleanup uses unsafe unmount %q: %s", forbidden, command)
		}
	}
}

func TestVolumeImportFilesystemCleanupHandlesVanishedMountinfoUnderErrexit(t *testing.T) {
	dir := t.TempDir()
	mountinfo := filepath.Join(dir, "123", "mountinfo")
	if err := os.Mkdir(filepath.Dir(mountinfo), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mountinfo, []byte("ignored\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{
		"awk": "#!/bin/sh\nrm -f \"$6\"\nexit 20\n",
		"zfs": "#!/bin/sh\ncase \"$1 $2\" in\n'list -H') exit 0 ;;\n'get -H') case \"$5\" in mountpoint) echo /testpool/import ;; mounted) echo no ;; esac ;;\n'destroy -r') : > \"$DESTROYED\" ;;\nesac\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	destroyed := filepath.Join(dir, "destroyed")
	command := strings.Replace(destroyImportedBackendCommand("testpool/import", true), "/proc/[0-9]*/mountinfo", filepath.Join(dir, "[0-9]*", "mountinfo"), 1)
	cmd := exec.Command("sh", "-ceu", command)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DESTROYED="+destroyed)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cleanup command failed after mountinfo disappeared: %v: %s", err, output)
	}
	if _, err := os.Stat(destroyed); err != nil {
		t.Fatalf("cleanup did not reach destroy after handled awk status: %v", err)
	}
}

func TestVolumeImportFilesystemCleanupRetriesBusyUnmountThenDestroys(t *testing.T) {
	dir := t.TempDir()
	writeVolumeImportCleanupZFSStub(t, dir, 1)

	destroyed := filepath.Join(dir, "destroyed")
	attempts := filepath.Join(dir, "unmount-attempts")
	command := strings.Replace(destroyImportedBackendCommandWithUnmountRetry("testpool/import", true, 2, 0), "/proc/[0-9]*/mountinfo", filepath.Join(dir, "[0-9]*", "mountinfo"), 1)
	cmd := exec.Command("sh", "-ceu", command)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DESTROYED="+destroyed, "UNMOUNT_ATTEMPTS="+attempts)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cleanup command did not recover from busy unmount: %v: %s", err, output)
	}
	if got := readUnmountAttempts(t, attempts); got != 2 {
		t.Fatalf("unmount attempts = %d, want 2; output: %s", got, output)
	}
	if _, err := os.Stat(destroyed); err != nil {
		t.Fatalf("cleanup did not destroy after successful retry: %v", err)
	}
	if !strings.Contains(string(output), "attempt 1/2 failed (status 1): cannot unmount: dataset is busy") {
		t.Fatalf("cleanup output does not describe busy retry: %s", output)
	}
}

func TestVolumeImportFilesystemCleanupFailsAfterBusyUnmountRetryLimit(t *testing.T) {
	dir := t.TempDir()
	writeVolumeImportCleanupZFSStub(t, dir, 99)

	destroyed := filepath.Join(dir, "destroyed")
	attempts := filepath.Join(dir, "unmount-attempts")
	command := strings.Replace(destroyImportedBackendCommandWithUnmountRetry("testpool/import", true, 2, 0), "/proc/[0-9]*/mountinfo", filepath.Join(dir, "[0-9]*", "mountinfo"), 1)
	cmd := exec.Command("sh", "-ceu", command)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DESTROYED="+destroyed, "UNMOUNT_ATTEMPTS="+attempts)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("cleanup command succeeded despite perpetual busy unmount: %s", output)
	}
	if got := readUnmountAttempts(t, attempts); got != 2 {
		t.Fatalf("unmount attempts = %d, want 2; output: %s", got, output)
	}
	if _, err := os.Stat(destroyed); !os.IsNotExist(err) {
		t.Fatalf("cleanup reached destroy after failed unmount: %v", err)
	}
	if !strings.Contains(string(output), "attempt 2/2 failed (status 1): cannot unmount: dataset is busy") {
		t.Fatalf("cleanup output does not describe terminal busy failure: %s", output)
	}
}

func TestVolumeImportFilesystemCleanupDoesNotRetryNonBusyUnmountError(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
case "$1 $2" in
'list -H') exit 0 ;;
'get -H')
  case "$5" in
  mountpoint) echo /testpool/import ;;
  mounted) echo yes ;;
  esac
  ;;
'unmount testpool/import')
  attempt=0
  [ ! -e "$UNMOUNT_ATTEMPTS" ] || attempt=$(cat "$UNMOUNT_ATTEMPTS")
  printf '%s\n' "$((attempt + 1))" > "$UNMOUNT_ATTEMPTS"
  echo 'cannot unmount: permission denied' >&2
  exit 3
  ;;
'destroy -r') : > "$DESTROYED" ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "zfs"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	destroyed := filepath.Join(dir, "destroyed")
	attempts := filepath.Join(dir, "unmount-attempts")
	command := strings.Replace(destroyImportedBackendCommandWithUnmountRetry("testpool/import", true, 2, 0), "/proc/[0-9]*/mountinfo", filepath.Join(dir, "[0-9]*", "mountinfo"), 1)
	cmd := exec.Command("sh", "-ceu", command)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DESTROYED="+destroyed, "UNMOUNT_ATTEMPTS="+attempts)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("cleanup command succeeded despite non-busy unmount error: %s", output)
	}
	if got := readUnmountAttempts(t, attempts); got != 1 {
		t.Fatalf("unmount attempts = %d, want 1; output: %s", got, output)
	}
	if _, err := os.Stat(destroyed); !os.IsNotExist(err) {
		t.Fatalf("cleanup reached destroy after non-busy unmount error: %v", err)
	}
}

func TestVolumeImportFilesystemCleanupRejectsUnsafeMountinfo(t *testing.T) {
	for _, test := range []struct {
		name      string
		mountinfo string
	}{
		{name: "live mount status 42", mountinfo: "36 25 0:32 / /testpool/import rw - zfs testpool/import rw\n"},
		{name: "malformed status 20", mountinfo: "malformed\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			mountinfo := filepath.Join(dir, "123", "mountinfo")
			if err := os.Mkdir(filepath.Dir(mountinfo), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(mountinfo, []byte(test.mountinfo), 0o644); err != nil {
				t.Fatal(err)
			}
			script := `#!/bin/sh
case "$1 $2" in
'list -H') exit 0 ;;
'get -H')
  case "$5" in
  mountpoint) echo /testpool/import ;;
  mounted) echo no ;;
  esac
  ;;
'destroy -r') : > "$DESTROYED" ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "zfs"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}

			destroyed := filepath.Join(dir, "destroyed")
			command := strings.Replace(destroyImportedBackendCommandWithUnmountRetry("testpool/import", true, 2, 0), "/proc/[0-9]*/mountinfo", filepath.Join(dir, "[0-9]*", "mountinfo"), 1)
			cmd := exec.Command("sh", "-ceu", command)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DESTROYED="+destroyed)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("cleanup command succeeded with unsafe mountinfo: %s", output)
			}
			if _, err := os.Stat(destroyed); !os.IsNotExist(err) {
				t.Fatalf("cleanup reached destroy with unsafe mountinfo: %v", err)
			}
		})
	}
}

func writeVolumeImportCleanupZFSStub(t *testing.T, dir string, busyAttempts int) {
	t.Helper()
	script := fmt.Sprintf(`#!/bin/sh
case "$1 $2" in
'list -H') exit 0 ;;
'get -H')
  case "$5" in
  mountpoint) echo /testpool/import ;;
  mounted) [ -e "$UNMOUNTED" ] && echo no || echo yes ;;
  esac
  ;;
'unmount testpool/import')
  attempt=0
  [ ! -e "$UNMOUNT_ATTEMPTS" ] || attempt=$(cat "$UNMOUNT_ATTEMPTS")
  attempt=$((attempt + 1))
  printf '%%s\n' "$attempt" > "$UNMOUNT_ATTEMPTS"
  if [ "$attempt" -le %d ]; then
    echo 'cannot unmount: dataset is busy' >&2
    exit 1
  fi
  : > "$UNMOUNTED"
  ;;
'destroy -r') : > "$DESTROYED" ;;
esac
`, busyAttempts)
	if err := os.WriteFile(filepath.Join(dir, "zfs"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("UNMOUNTED", filepath.Join(dir, "unmounted"))
}

func readUnmountAttempts(t *testing.T, path string) int {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var attempts int
	if _, err := fmt.Sscanf(string(contents), "%d", &attempts); err != nil {
		t.Fatal(err)
	}
	return attempts
}

func TestVolumeImportCleanupStopsWhenConsumersCannotBeDeleted(t *testing.T) {
	fixture := volumeImportFixture{volume: &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "filesystem"}}}
	base := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).Build()
	c := &recordingDeleteClient{Client: base, podErr: errors.New("pod deletion uncertain")}
	err := cleanupVolumeImportScenario(c, storageNode{}, "imports", "testpool/import", []volumeImportFixture{fixture}, nil)
	if err == nil || len(c.deleted) != 1 || c.deleted[0] != "*v1.Pod/imports/filesystem-consumer" {
		t.Fatalf("cleanup error=%v deletes=%v, want only consumer delete", err, c.deleted)
	}
}

func TestWaitForImportClaimsAndAttachmentsGoneWaitsForMatchingAttachment(t *testing.T) {
	scheme := newSchemeForTest(t)
	pvName := "import-pv"
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attached"},
		Spec:       storagev1.VolumeAttachmentSpec{Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(attachment).Build()
	c := &oneShotAttachmentClient{Client: base}
	if err := waitForImportClaimsAndAttachmentsGone(context.Background(), c, nil, []string{pvName}); err != nil {
		t.Fatalf("wait for attachment: %v", err)
	}
	if c.lists < 2 {
		t.Fatalf("attachment lists = %d, want at least two", c.lists)
	}
}

func TestImportedConsumersUseRunScopedNamespaceAndLogTerminationMessages(t *testing.T) {
	fixtures := volumeImportFixtures("testpool", "testpool/e2e-volume-import-run", "storage")
	namespace := makeImportConsumerNamespace("run-123")
	if namespace != "zfs-csi-import-run-123" {
		t.Fatalf("consumer namespace = %q", namespace)
	}
	for _, fixture := range fixtures {
		pod := importedConsumer(namespace, fixture, false)
		if pod.Namespace != namespace || len(pod.Spec.Containers) != 1 ||
			pod.Spec.Containers[0].TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Fatalf("consumer %s does not preserve failure logs: %#v", fixture.name, pod.Spec)
		}
		security := pod.Spec.Containers[0].SecurityContext
		if fixture.kind == "filesystem" {
			if security == nil || security.RunAsUser == nil || *security.RunAsUser != 65534 || security.RunAsGroup == nil || *security.RunAsGroup != 65534 {
				t.Fatalf("filesystem consumer must use NFS root-squash identity: %#v", security)
			}
		} else if security != nil {
			t.Fatalf("block consumer unexpectedly changes run identity: %#v", security)
		}
	}
}

func TestDeleteImportedConsumersWaitsForPodDisappearanceBeforeProgressing(t *testing.T) {
	ctx := context.Background()
	namespace := "import-test"
	fixture := volumeImportFixture{volume: &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "filesystem"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: importedConsumerName(fixture), Namespace: namespace}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "filesystem-claim", Namespace: namespace}}
	base := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(pod, pvc).Build()
	c := &delayedPodDeletionClient{Client: base, pendingPods: map[client.ObjectKey]bool{}}

	if err := deleteImportedConsumers(ctx, c, namespace, []volumeImportFixture{fixture}); err != nil {
		t.Fatalf("delete imported consumers: %v", err)
	}
	if err := c.Delete(ctx, pvc); err != nil {
		t.Fatalf("delete claim after consumer teardown: %v", err)
	}
	for _, obj := range []client.Object{pod, pvc} {
		if err := base.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object)); !apierrors.IsNotFound(err) {
			t.Fatalf("expected %T %s to be deleted, got %v", obj, client.ObjectKeyFromObject(obj), err)
		}
	}
}

type recordingDeleteClient struct {
	client.Client
	podErr  error
	deleted []string
}

func (c *recordingDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deleted = append(c.deleted, fmt.Sprintf("%T/%s/%s", obj, obj.GetNamespace(), obj.GetName()))
	if _, ok := obj.(*corev1.Pod); ok {
		return c.podErr
	}
	return c.Client.Delete(ctx, obj, opts...)
}

type oneShotAttachmentClient struct {
	client.Client
	lists int
}

func (c *oneShotAttachmentClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists++
	if c.lists == 2 {
		attachments := &storagev1.VolumeAttachmentList{}
		if err := c.Client.List(ctx, attachments); err != nil {
			return err
		}
		for i := range attachments.Items {
			if err := c.Client.Delete(ctx, &attachments.Items[i]); err != nil {
				return err
			}
		}
	}
	return c.Client.List(ctx, list, opts...)
}
