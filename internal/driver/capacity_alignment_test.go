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

package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	testGiB     = int64(1) << 30
	test16K     = int64(16 * 1024)
	test128K    = int64(128 * 1024)
	testDefault = int64(16 * 1024) // ZFS default volblocksize the driver aligns to
)

func blockCapabilities() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
}

func fsCapabilities() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}}
}

// TestCreateVolumeAlignsBlockCapacityToVolBlockSize is the direct regression for
// issue #1: a zvol request whose bytes are not divisible by volblocksize must be
// rounded up so the persisted capacity is a valid volsize.
func TestCreateVolumeAlignsBlockCapacityToVolBlockSize(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-unaligned")

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-unaligned",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := testGiB + test16K
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-unaligned"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != want {
		t.Fatalf("Volume spec capacity = %d, want %d (aligned up to 16k)", got.Spec.Capacity, want)
	}
	if got.Spec.Capacity%test16K != 0 {
		t.Fatalf("persisted capacity %d not divisible by volblocksize", got.Spec.Capacity)
	}
	if resp.GetVolume().GetCapacityBytes() != want {
		t.Fatalf("response capacity = %d, want %d", resp.GetVolume().GetCapacityBytes(), want)
	}
}

// TestCreateVolumeAlignsToDefaultVolBlockSizeWhenUnset proves an unset blocksize
// parameter still yields a volsize the ZFS default volblocksize accepts, and that
// the driver persists that default explicitly instead of leaving volBlockSize
// empty: expansion reads the persisted value, so an implicit (build-dependent)
// ZFS default could otherwise make create and expand disagree.
func TestCreateVolumeAlignsToDefaultVolBlockSizeWhenUnset(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-default-align")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-default-align",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: blockCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-default-align"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+testDefault {
		t.Fatalf("Volume spec capacity = %d, want %d", got.Spec.Capacity, testGiB+testDefault)
	}
	if got.Spec.VolBlockSize != zfs.DefaultVolBlockSizeValue {
		t.Fatalf("Volume spec volBlockSize = %q, want explicit %q", got.Spec.VolBlockSize, zfs.DefaultVolBlockSizeValue)
	}
}

// TestCreateVolumeCanonicalisesBlockSize proves equivalent spellings of the same
// volblocksize persist identically, so a same-name retry that spells the block
// size differently is still recognised as the same volume.
func TestCreateVolumeCanonicalisesBlockSize(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blocksize string
		want      string
		wantBytes int64
	}{
		{name: "pvc-canon-512", blocksize: "512", want: "512", wantBytes: 512},
		{name: "pvc-canon-1k", blocksize: "1024", want: "1k", wantBytes: 1024},
		{name: "pvc-canon-upper", blocksize: "16K", want: "16k", wantBytes: test16K},
		{name: "pvc-canon-bytes", blocksize: "16384", want: "16k", wantBytes: test16K},
		{name: "pvc-canon-128k", blocksize: "128k", want: "128k", wantBytes: test128K},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t)
			cs := newTestController(c)
			go autoReady(t, c, tc.name)

			if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               tc.name,
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
				Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": tc.blocksize},
				VolumeCapabilities: blockCapabilities(),
			}); err != nil {
				t.Fatalf("CreateVolume: %v", err)
			}

			got := &zfscsiv1.Volume{}
			if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: tc.name}, got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.VolBlockSize != tc.want {
				t.Fatalf("volBlockSize = %q, want canonical %q", got.Spec.VolBlockSize, tc.want)
			}
			if got.Spec.Capacity != testGiB+tc.wantBytes {
				t.Fatalf("capacity = %d, want %d", got.Spec.Capacity, testGiB+tc.wantBytes)
			}
		})
	}
}

// TestCreateVolumeRejectsBlockSizesOpenZFSCannotUse proves the driver rejects
// volblocksize values a zvol create would fail on (non power of two, below 512
// bytes, above 128 KiB) before any Volume CR is written.
func TestCreateVolumeRejectsBlockSizesOpenZFSCannotUse(t *testing.T) {
	for _, blocksize := range []string{"256", "511", "768", "12k", "100k", "256k", "1m", "1g"} {
		t.Run(blocksize, func(t *testing.T) {
			c := newTestClient(t)
			cs := newTestController(c)
			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-bad-volblocksize",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": blocksize},
				VolumeCapabilities: blockCapabilities(),
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("blocksize=%q error = %v (code %s), want InvalidArgument", blocksize, err, status.Code(err))
			}
			if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-bad-volblocksize"}, &zfscsiv1.Volume{}); err == nil {
				t.Fatal("rejected request must not persist a Volume CR")
			}
		})
	}
}

// TestCreateVolumeFilesystemAcceptsNonZvolBlockSizes proves the zvol-specific
// volblocksize bounds do not apply to dataset recordsize, which accepts larger
// values and does not constrain byte-exact refquota capacity.
func TestCreateVolumeFilesystemAcceptsNonZvolBlockSizes(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-fs-recordsize")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-fs-recordsize",
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters: map[string]string{
			"pool": "tank", "type": "filesystem", "blocksize": "1M",
			"nfsExportCIDRs": "10.0.0.0/8",
		},
		VolumeCapabilities: fsCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-fs-recordsize"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.VolBlockSize != "1M" {
		t.Fatalf("filesystem volBlockSize = %q, want byte-exact passthrough %q", got.Spec.VolBlockSize, "1M")
	}
	if got.Spec.Capacity != testGiB+1 {
		t.Fatalf("filesystem capacity = %d, want byte-exact %d", got.Spec.Capacity, testGiB+1)
	}
}

// TestCreateVolumeSameNameDifferentBlockSizeIsAlreadyExists proves the CSI
// idempotency gate covers effective block size: retrying the same volume name
// with a block size that would provision a differently-aligned zvol must fail
// rather than silently return the existing volume.
func TestCreateVolumeSameNameDifferentBlockSizeIsAlreadyExists(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-blocksize-retry")

	request := func(blocksize string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:               "pvc-blocksize-retry",
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": blocksize},
			VolumeCapabilities: blockCapabilities(),
		}
	}
	if _, err := cs.CreateVolume(context.Background(), request("16k")); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// A retry spelling the same block size differently is a legitimate retry.
	if _, err := cs.CreateVolume(context.Background(), request("16K")); err != nil {
		t.Fatalf("CreateVolume idempotent retry with equivalent block size: %v", err)
	}

	if _, err := cs.CreateVolume(context.Background(), request("128k")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateVolume error = %v (code %s), want AlreadyExists for differing block size", err, status.Code(err))
	}
}

// TestCreateVolumeSameNameRetryAfterDefaultedBlockSize proves a retry that omits
// the blocksize parameter matches a volume created with the explicit default,
// and a legacy CR with no persisted volBlockSize stays compatible with it.
func TestCreateVolumeSameNameRetryAfterDefaultedBlockSize(t *testing.T) {
	c := newTestClient(t)
	legacy := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-legacy-blocksize"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB,
			VolumeID: "csi:tank:block:pvc-legacy-blocksize", VolName: "pvc-legacy-blocksize",
		},
	}
	if err := c.Create(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	mustSetReady(t, c, legacy.Name)
	cs := newTestController(c)

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-legacy-blocksize",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: blockCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume retry against legacy CR with inherited block size: %v", err)
	}

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-legacy-blocksize",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "128k"},
		VolumeCapabilities: blockCapabilities(),
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateVolume error = %v (code %s), want AlreadyExists for differing block size", err, status.Code(err))
	}
}

// TestCreateVolumeFilesystemCapacityIsByteExact proves dataset refquota is not
// rounded: recordsize does not constrain refquota.
func TestCreateVolumeFilesystemCapacityIsByteExact(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-fs-exact")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-fs-exact",
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters: map[string]string{
			"pool": "tank", "type": "filesystem", "blocksize": "128k",
			"nfsExportCIDRs": "10.0.0.0/8",
		},
		VolumeCapabilities: fsCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-fs-exact"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+1 {
		t.Fatalf("filesystem capacity = %d, want byte-exact %d", got.Spec.Capacity, testGiB+1)
	}
}

// TestCreateVolumeRejectsWhenAlignedCapacityExceedsLimit preserves the CSI
// CapacityRange contract: no aligned capacity fits, so the request is rejected
// rather than silently returning a volume larger than limit_bytes.
func TestCreateVolumeRejectsWhenAlignedCapacityExceedsLimit(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-limit",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1, LimitBytes: testGiB + 2},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume error = %v (code %s), want InvalidArgument", err, status.Code(err))
	}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-limit"}, &zfscsiv1.Volume{}); err == nil {
		t.Fatal("rejected request must not persist a Volume CR")
	}
}

// TestCreateVolumeAcceptsAlignedCapacityWithinLimit proves rounding is allowed to
// consume headroom up to and including limit_bytes.
func TestCreateVolumeAcceptsAlignedCapacityWithinLimit(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-limit-ok")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-limit-ok",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1, LimitBytes: testGiB + test16K},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-limit-ok"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+test16K {
		t.Fatalf("capacity = %d, want %d", got.Spec.Capacity, testGiB+test16K)
	}
}

// TestCreateVolumeRejectsRequiredAboveLimit rejects an incoherent CapacityRange.
func TestCreateVolumeRejectsRequiredAboveLimit(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-bad-range",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: testGiB - 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume error = %v (code %s), want InvalidArgument", err, status.Code(err))
	}
}

// TestCreateVolumeRejectsInvalidBlockSizeParameter proves blocksize syntax is
// validated in the driver (overflow and malformed values) before a CR is written.
func TestCreateVolumeRejectsInvalidBlockSizeParameter(t *testing.T) {
	for _, blocksize := range []string{"0", "16t", "9223372036854775807k", "abc"} {
		c := newTestClient(t)
		cs := newTestController(c)
		_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "pvc-bad-blocksize",
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": blocksize},
			VolumeCapabilities: blockCapabilities(),
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("blocksize=%q error = %v (code %s), want InvalidArgument", blocksize, err, status.Code(err))
		}
	}
}

// TestCreateVolumeReservesAlignedCapacity proves placement accounts for the
// rounded-up capacity, not the smaller requested capacity.
func TestCreateVolumeReservesAlignedCapacity(t *testing.T) {
	c := newTestClient(t)
	testPoolResolverWithFree(c, "node-a", "tank", "1", testGiB+test16K-1)
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system", Portal: "server7:4420"})

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-reserve",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("CreateVolume error = %v (code %s), want ResourceExhausted for aligned reservation", err, status.Code(err))
	}
}

// TestCreateVolumeCloneAlignsToSourceBlockSize proves a clone accounts for the
// inherited volblocksize: zfs clone keeps the source's volblocksize regardless of
// the StorageClass blocksize parameter.
func TestCreateVolumeCloneAlignsToSourceBlockSize(t *testing.T) {
	c := newTestClient(t)
	source := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "clone-src"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB, VolBlockSize: "128k",
			VolumeID: "csi:tank:block:clone-src", VolName: "clone-src",
		},
	}
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)
	go autoReady(t, c, "pvc-clone")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-clone",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: source.Spec.VolumeID}},
		},
	}); err != nil {
		t.Fatalf("CreateVolume clone: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-clone"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+test128K {
		t.Fatalf("clone capacity = %d, want %d (aligned to inherited 128k)", got.Spec.Capacity, testGiB+test128K)
	}
	if got.Spec.VolBlockSize != "128k" {
		t.Fatalf("clone volBlockSize = %q, want inherited 128k", got.Spec.VolBlockSize)
	}
}

// TestCreateVolumeSnapshotRestoreAlignsToParentBlockSize proves a restore aligns
// to the snapshot parent's volblocksize, which the restored clone inherits.
func TestCreateVolumeSnapshotRestoreAlignsToParentBlockSize(t *testing.T) {
	c := newTestClient(t)
	parent := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-parent"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB, VolBlockSize: "128k",
			VolumeID: "csi:tank:block:snap-parent", VolName: "snap-parent",
		},
	}
	if err := c.Create(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-a"},
		Spec: zfscsiv1.SnapshotSpec{
			PoolGUID: "1", VolumeRef: parent.Name, SourceVolumeID: parent.Spec.VolumeID,
			SnapName: "snap-a", SnapshotID: parent.Spec.VolumeID + "@snap-a", OwnerNode: "server7",
		},
	}
	if err := c.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)
	go autoReady(t, c, "pvc-restore")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-restore",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.Spec.SnapshotID}},
		},
	}); err != nil {
		t.Fatalf("CreateVolume restore: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-restore"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+test128K {
		t.Fatalf("restore capacity = %d, want %d (aligned to parent 128k)", got.Spec.Capacity, testGiB+test128K)
	}
	if got.Spec.VolBlockSize != "128k" {
		t.Fatalf("restore volBlockSize = %q, want inherited 128k", got.Spec.VolBlockSize)
	}
}

// TestCreateVolumeSnapshotRestoreUsesRecordedSourceBlockSize proves the restore
// aligns to the block size recorded on the Snapshot CR when the parent Volume CR
// is gone (a retained parent whose CR was removed). Without the recorded value
// the restore would align to the StorageClass block size and provision a zvol
// whose volsize is illegal for the volblocksize it inherits from the clone origin.
func TestCreateVolumeSnapshotRestoreUsesRecordedSourceBlockSize(t *testing.T) {
	c := newTestClient(t)
	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-orphaned"},
		Spec: zfscsiv1.SnapshotSpec{
			PoolGUID: "1", VolumeRef: "gone-parent", SourceVolumeID: "csi:tank:block:gone-parent",
			SnapName: "snap-orphaned", SnapshotID: "csi:tank:block:gone-parent@snap-orphaned",
			OwnerNode: "server7", SourceVolBlockSize: "128k",
		},
	}
	if err := c.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)
	go autoReady(t, c, "pvc-restore-orphaned")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-restore-orphaned",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.Spec.SnapshotID}},
		},
	}); err != nil {
		t.Fatalf("CreateVolume restore: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-restore-orphaned"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.VolBlockSize != "128k" {
		t.Fatalf("restore volBlockSize = %q, want recorded source 128k", got.Spec.VolBlockSize)
	}
	if got.Spec.Capacity != testGiB+test128K {
		t.Fatalf("restore capacity = %d, want %d (aligned to recorded 128k)", got.Spec.Capacity, testGiB+test128K)
	}
}

// TestCreateSnapshotRecordsSourceBlockSize proves CreateSnapshot copies the
// source volume's block size onto the Snapshot CR, which is what makes the
// restore fallback above authoritative.
func TestCreateSnapshotRecordsSourceBlockSize(t *testing.T) {
	c := newTestClient(t)
	source := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "snapsrc"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB, VolBlockSize: "128k",
			VolumeID: "csi:tank:block:snapsrc", VolName: "snapsrc",
		},
	}
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)
	go autoReadySnapshot(t, c, "snap-recorded")

	if _, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-recorded",
		SourceVolumeId: source.Spec.VolumeID,
	}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got := &zfscsiv1.Snapshot{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "snap-recorded"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.SourceVolBlockSize != "128k" {
		t.Fatalf("Snapshot spec.sourceVolBlockSize = %q, want source 128k", got.Spec.SourceVolBlockSize)
	}
}

// autoReadySnapshot polls for the Snapshot CR by name and marks it ReadyToUse,
// standing in for the storage-agent reconciler.
func autoReadySnapshot(t *testing.T, c crclient.Client, name string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		snap := &zfscsiv1.Snapshot{}
		if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: name}, snap); err == nil {
			before := snap.DeepCopy()
			snap.Status.State = zfscsiv1.SnapshotStateReady
			snap.Status.ReadyToUse = true
			if err := c.Status().Patch(context.Background(), snap, crclient.MergeFrom(before)); err != nil {
				t.Errorf("mark snapshot ready: %v", err)
			}

			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("autoReadySnapshot never observed CR %q", name)
}

func newExpandVolume(t *testing.T, c crclient.Client, name, blockSize string, capacity int64, kind zfscsiv1.VolumeType) *zfscsiv1.Volume {
	t.Helper()
	sample := metav1.Now()
	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: kind,
			VolumeID: "csi:tank:" + string(kind) + ":" + name, VolName: name,
			Capacity: capacity, VolBlockSize: blockSize,
		},
		Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: &sample},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	return v
}

// TestControllerExpandAlignsToVolBlockSize proves expansion uses the volume's
// persisted (effective) volblocksize, so `zfs set volsize` always gets a legal value.
func TestControllerExpandAlignsToVolBlockSize(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := newExpandVolume(t, c, "grow-aligned", "16k", testGiB, zfscsiv1.VolumeTypeBlock)

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      v.Spec.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1},
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	want := testGiB + test16K
	if resp.GetCapacityBytes() != want {
		t.Fatalf("expand response capacity = %d, want %d", resp.GetCapacityBytes(), want)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != want {
		t.Fatalf("expanded capacity = %d, want %d", got.Spec.Capacity, want)
	}
}

// TestControllerExpandRejectsWhenAlignedCapacityExceedsLimit keeps the CSI
// capacity-range contract on the expansion path.
func TestControllerExpandRejectsWhenAlignedCapacityExceedsLimit(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := newExpandVolume(t, c, "grow-limited", "16k", testGiB, zfscsiv1.VolumeTypeBlock)

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      v.Spec.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1, LimitBytes: testGiB + 2},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expand error = %v (code %s), want InvalidArgument", err, status.Code(err))
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB {
		t.Fatalf("rejected expansion mutated capacity: %d", got.Spec.Capacity)
	}
}

// TestControllerExpandIdempotentWhenAlignedCapacityAlreadySatisfied proves an
// already-aligned volume treats a smaller unaligned request as satisfied instead
// of shrinking or re-patching.
func TestControllerExpandIdempotentWhenAlignedCapacityAlreadySatisfied(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := newExpandVolume(t, c, "grow-satisfied", "16k", testGiB+test16K, zfscsiv1.VolumeTypeBlock)

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      v.Spec.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1},
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if resp.GetCapacityBytes() != testGiB+test16K {
		t.Fatalf("expand response capacity = %d, want %d", resp.GetCapacityBytes(), testGiB+test16K)
	}
}

// TestControllerExpandFilesystemCapacityIsByteExact proves refquota expansion is
// not rounded.
func TestControllerExpandFilesystemCapacityIsByteExact(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := newExpandVolume(t, c, "grow-fs", "128k", testGiB, zfscsiv1.VolumeTypeFilesystem)

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      v.Spec.VolumeID,
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB + 1},
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if resp.GetCapacityBytes() != testGiB+1 {
		t.Fatalf("filesystem expand capacity = %d, want byte-exact %d", resp.GetCapacityBytes(), testGiB+1)
	}
}

// TestCreateVolumeSameNameRetryWithinCapacityRangeIsIdempotent proves the CSI
// idempotency gate compares the persisted capacity against the retry's
// CapacityRange, not against the driver's own aligned capacity.
//
// The driver rounds a block volume UP to the next volblocksize multiple, so the
// persisted capacity is routinely LARGER than required_bytes. An exact-equality
// check would then reject the provisioner's own retry of the identical PVC —
// the retry still asks for the unaligned required_bytes, which never equals the
// aligned capacity on disk. CSI requires AlreadyExists only for an INCOMPATIBLE
// request, and a volume at or above required_bytes is compatible.
func TestCreateVolumeSameNameRetryWithinCapacityRangeIsIdempotent(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-range-retry")

	request := func(required, limit int64) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:               "pvc-range-retry",
			CapacityRange:      &csi.CapacityRange{RequiredBytes: required, LimitBytes: limit},
			Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
			VolumeCapabilities: blockCapabilities(),
		}
	}

	// Unaligned request: provisions testGiB+16K.
	if _, err := cs.CreateVolume(context.Background(), request(testGiB+1, 0)); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-range-retry"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != testGiB+test16K {
		t.Fatalf("persisted capacity = %d, want aligned %d", got.Spec.Capacity, testGiB+test16K)
	}

	for _, tc := range []struct {
		name     string
		required int64
		limit    int64
	}{
		{name: "identical retry of the unaligned request", required: testGiB + 1},
		{name: "retry asking for exactly the aligned capacity", required: testGiB + test16K},
		{name: "retry asking for strictly less", required: testGiB},
		{name: "retry whose limit admits the aligned capacity", required: testGiB + 1, limit: testGiB + test16K},
		{name: "retry whose limit is above the aligned capacity", required: testGiB + 1, limit: 4 * testGiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := cs.CreateVolume(context.Background(), request(tc.required, tc.limit)); err != nil {
				t.Fatalf("compatible retry rejected: %v (code %s)", err, status.Code(err))
			}
		})
	}

	// The persisted volume must not have been mutated by any retry.
	after := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-range-retry"}, after); err != nil {
		t.Fatal(err)
	}
	if after.Spec.Capacity != testGiB+test16K {
		t.Fatalf("retry mutated capacity: %d, want %d", after.Spec.Capacity, testGiB+test16K)
	}
}

// TestCreateVolumeSameNameRetryOutsideCapacityRangeReturnsAlreadyExists proves the
// range check still rejects genuinely incompatible retries in both directions:
// a larger required_bytes the existing volume cannot satisfy, and a limit_bytes
// the existing volume exceeds.
func TestCreateVolumeSameNameRetryOutsideCapacityRangeReturnsAlreadyExists(t *testing.T) {
	for _, tc := range []struct {
		name     string
		required int64
		limit    int64
	}{
		{name: "required above existing capacity", required: 4 * testGiB},
		{name: "required one byte above existing capacity", required: testGiB + test16K + 1},
		{name: "limit below existing capacity", required: testGiB, limit: testGiB + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t)
			cs := newTestController(c)
			name := "pvc-range-" + sanitizeID(tc.name)
			go autoReady(t, c, name)

			request := func(required, limit int64) *csi.CreateVolumeRequest {
				return &csi.CreateVolumeRequest{
					Name:               name,
					CapacityRange:      &csi.CapacityRange{RequiredBytes: required, LimitBytes: limit},
					Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
					VolumeCapabilities: blockCapabilities(),
				}
			}
			if _, err := cs.CreateVolume(context.Background(), request(testGiB+1, 0)); err != nil {
				t.Fatalf("CreateVolume: %v", err)
			}
			if _, err := cs.CreateVolume(context.Background(), request(tc.required, tc.limit)); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("error = %v (code %s), want AlreadyExists", err, status.Code(err))
			}
		})
	}
}

// TestCreateVolumeFilesystemSameNameRetryUsesCapacityRange proves the same range
// semantics apply to byte-exact filesystem capacity: a retry asking for less than
// the provisioned refquota is satisfied, and one asking for more is not.
func TestCreateVolumeFilesystemSameNameRetryUsesCapacityRange(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-fs-range-retry")

	request := func(required int64) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:          "pvc-fs-range-retry",
			CapacityRange: &csi.CapacityRange{RequiredBytes: required},
			Parameters: map[string]string{
				"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/8",
			},
			VolumeCapabilities: fsCapabilities(),
		}
	}
	if _, err := cs.CreateVolume(context.Background(), request(testGiB)); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := cs.CreateVolume(context.Background(), request(testGiB-1)); err != nil {
		t.Fatalf("smaller compatible filesystem retry rejected: %v", err)
	}
	if _, err := cs.CreateVolume(context.Background(), request(testGiB+1)); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("larger filesystem retry error = %v (code %s), want AlreadyExists", err, status.Code(err))
	}
}

// TestCreateVolumeRetryStillRejectsLimitBelowAlignedCapacity proves the alignment
// policy is preserved on the retry path: a retry whose limit_bytes cannot admit
// any aligned capacity is rejected with InvalidArgument by the alignment gate
// before the idempotency comparison is ever reached.
func TestCreateVolumeRetryStillRejectsLimitBelowAlignedCapacity(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-retry-limit")

	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-retry-limit",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-retry-limit",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1, LimitBytes: testGiB + 2},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error = %v (code %s), want InvalidArgument from the alignment gate", err, status.Code(err))
	}
}

// TestCreateVolumeRejectsBlockCloneWithoutAuthoritativeSourceBlockSize proves the
// driver refuses to derive a zvol from a block source whose volblocksize is not
// recorded. `zfs clone` inherits volblocksize from its origin, and the controller
// cannot read ZFS properties, so assuming the modern 16 KiB default would silently
// mis-align the clone against a source created under a different (e.g. legacy
// 8 KiB) default.
func TestCreateVolumeRejectsBlockCloneWithoutAuthoritativeSourceBlockSize(t *testing.T) {
	c := newTestClient(t)
	legacy := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-src"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB,
			VolumeID: "csi:tank:block:legacy-src", VolName: "legacy-src",
		},
	}
	if err := c.Create(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-legacy-clone",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
		VolumeCapabilities: blockCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: legacy.Spec.VolumeID}},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("clone error = %v (code %s), want FailedPrecondition", err, status.Code(err))
	}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-legacy-clone"}, &zfscsiv1.Volume{}); err == nil {
		t.Fatal("rejected clone must not persist a Volume CR")
	}
}

// TestCreateVolumeRejectsBlockRestoreWithoutAuthoritativeSourceBlockSize proves the
// same guard on the snapshot-restore path, for both an orphaned legacy Snapshot CR
// and one whose parent Volume CR itself records no block size.
func TestCreateVolumeRejectsBlockRestoreWithoutAuthoritativeSourceBlockSize(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withParent   bool
		snapshotName string
	}{
		{name: "orphaned legacy snapshot", snapshotName: "snap-legacy-orphan"},
		{name: "parent volume without block size", withParent: true, snapshotName: "snap-legacy-parent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t)
			volumeRef := "legacy-parent"
			if tc.withParent {
				parent := &zfscsiv1.Volume{
					ObjectMeta: metav1.ObjectMeta{Name: volumeRef},
					Spec: zfscsiv1.VolumeSpec{
						Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
						Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB,
						VolumeID: "csi:tank:block:legacy-parent", VolName: volumeRef,
					},
				}
				if err := c.Create(context.Background(), parent); err != nil {
					t.Fatal(err)
				}
			}
			snap := &zfscsiv1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{Name: tc.snapshotName},
				Spec: zfscsiv1.SnapshotSpec{
					PoolGUID: "1", VolumeRef: volumeRef, SourceVolumeID: "csi:tank:block:legacy-parent",
					SnapName: tc.snapshotName, SnapshotID: "csi:tank:block:legacy-parent@" + tc.snapshotName,
					OwnerNode: "server7",
				},
			}
			if err := c.Create(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
			cs := newTestController(c)

			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-" + tc.snapshotName,
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:         map[string]string{"pool": "tank", "type": "block", "blocksize": "16k"},
				VolumeCapabilities: blockCapabilities(),
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.Spec.SnapshotID}},
				},
			})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("restore error = %v (code %s), want FailedPrecondition", err, status.Code(err))
			}
			if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-" + tc.snapshotName}, &zfscsiv1.Volume{}); err == nil {
				t.Fatal("rejected restore must not persist a Volume CR")
			}
		})
	}
}

// TestCreateSnapshotRejectsBlockSourceWithoutAuthoritativeBlockSize proves
// CreateSnapshot refuses to record an empty block size for a block source rather
// than persisting an unusable Snapshot CR that later restores would misalign
// against (or silently treat as the 16 KiB default).
func TestCreateSnapshotRejectsBlockSourceWithoutAuthoritativeBlockSize(t *testing.T) {
	c := newTestClient(t)
	source := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-snapsrc"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeBlock, Capacity: testGiB,
			VolumeID: "csi:tank:block:legacy-snapsrc", VolName: "legacy-snapsrc",
		},
	}
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)

	_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-legacy",
		SourceVolumeId: source.Spec.VolumeID,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateSnapshot error = %v (code %s), want FailedPrecondition", err, status.Code(err))
	}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "snap-legacy"}, &zfscsiv1.Snapshot{}); err == nil {
		t.Fatal("rejected snapshot must not persist a Snapshot CR with an empty source block size")
	}
}

// TestFilesystemSourcesRemainValidWithoutBlockSize proves the block-source guard
// does not touch filesystem volumes, whose empty volBlockSize is intentional:
// dataset recordsize places no constraint on refquota, so clone, restore and
// snapshot of a filesystem source all still succeed.
func TestFilesystemSourcesRemainValidWithoutBlockSize(t *testing.T) {
	c := newTestClient(t)
	source := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "fs-src"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers",
			Type: zfscsiv1.VolumeTypeFilesystem, Capacity: testGiB,
			VolumeID: "csi:tank:filesystem:fs-src", VolName: "fs-src",
			NFSExportCIDRs: []string{"10.0.0.0/8"}, NFSExportAccessMode: "rw",
		},
	}
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	cs := newTestController(c)

	go autoReadySnapshot(t, c, "fs-snap")
	if _, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "fs-snap",
		SourceVolumeId: source.Spec.VolumeID,
	}); err != nil {
		t.Fatalf("CreateSnapshot of filesystem source: %v", err)
	}
	snap := &zfscsiv1.Snapshot{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "fs-snap"}, snap); err != nil {
		t.Fatal(err)
	}
	if snap.Spec.SourceVolBlockSize != "" {
		t.Fatalf("filesystem snapshot sourceVolBlockSize = %q, want empty", snap.Spec.SourceVolBlockSize)
	}

	fsParams := map[string]string{"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/8"}
	go autoReady(t, c, "pvc-fs-clone")
	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-fs-clone",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         fsParams,
		VolumeCapabilities: fsCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: source.Spec.VolumeID}},
		},
	}); err != nil {
		t.Fatalf("CreateVolume clone of filesystem source: %v", err)
	}
	clone := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-fs-clone"}, clone); err != nil {
		t.Fatal(err)
	}
	if clone.Spec.Capacity != testGiB+1 {
		t.Fatalf("filesystem clone capacity = %d, want byte-exact %d", clone.Spec.Capacity, testGiB+1)
	}

	go autoReady(t, c, "pvc-fs-restore")
	if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-fs-restore",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB + 1},
		Parameters:         fsParams,
		VolumeCapabilities: fsCapabilities(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.Spec.SnapshotID}},
		},
	}); err != nil {
		t.Fatalf("CreateVolume restore of filesystem snapshot: %v", err)
	}
	restored := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-fs-restore"}, restored); err != nil {
		t.Fatal(err)
	}
	if restored.Spec.Capacity != testGiB+1 {
		t.Fatalf("filesystem restore capacity = %d, want byte-exact %d", restored.Spec.Capacity, testGiB+1)
	}
}
