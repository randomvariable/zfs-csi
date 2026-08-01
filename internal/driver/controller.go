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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"github.com/randomvariable/zfs-csi/internal/nfs"
	"github.com/randomvariable/zfs-csi/internal/psk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/capacity"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/placement"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	pollInterval             = 100 * time.Millisecond
	pollFallbackDeadline     = 5 * time.Second
	pollDeadlineSafetyMargin = 250 * time.Millisecond
	maxCRNameLen             = 250

	publishContextTargetNQN     = "target_nqn"
	publishContextPortal        = "portal"
	publishContextNamespaceID   = "namespace_id"
	publishContextDeviceGUID    = "device_guid"
	publishContextExportPath    = "exportPath"
	publishContextNFSServer     = "nfs_server"
	publishContextNFSRootPath   = "nfs_root_path"
	publishContextTLS           = "tls"
	publishContextPSKSecret     = "psk_secret"
	publishContextNetworkDomain = "network_domain"
	publishContextProvenance    = "provenance"
	defaultPlacementLeaseName   = "zfs-csi-placement"
	nvmeTLSPSKSecretPrefix      = "zfs-csi-nvme-psk-"
	nvmeTLSPSKSecretDataKey     = "psk"
)

// ControllerServer implements the CSI Controller service as a pure CR writer.
// It never touches ZFS or the transport directly — it writes Volume/Snapshot
// CRs and waits for the storage-agent reconciler to report Ready (PLAN §1).
type ControllerServer struct {
	csi.UnimplementedControllerServer

	log    logr.Logger
	client crclient.Client
	// namespace contains namespaced supporting objects such as capacity ConfigMaps.
	namespace          string
	apiReader          crclient.Reader
	placementLeaseName string
	// portal is the host:port consumers connect to for the block transport.
	portal string
	// encryptionKeyRefPrefix is the OpenBao path prefix for per-volume DEKs
	// (e.g. "transit/"); empty disables encryption on create.
	encryptionKeyRefPrefix string
	keys                   crypto.KeyProvider
	pskReader              io.Reader
}

// ControllerConfig configures the ControllerServer.
type ControllerConfig struct {
	Log                logr.Logger
	Client             crclient.Client
	Namespace          string
	Portal             string
	EncryptPrefix      string
	Keys               crypto.KeyProvider
	APIReader          crclient.Reader
	PlacementLeaseName string
	// PSKReader supplies configured-PSK entropy. It may be called concurrently
	// by CreateVolume RPCs and therefore must be safe for concurrent use.
	// Production defaults to crypto/rand.Reader, which satisfies this contract.
	PSKReader io.Reader
}

// NewControllerServer constructs a ControllerServer.
func NewControllerServer(cfg ControllerConfig) *ControllerServer {
	pskReader := cfg.PSKReader
	if pskReader == nil {
		pskReader = rand.Reader
	}
	return &ControllerServer{
		log:                    cfg.Log,
		client:                 cfg.Client,
		namespace:              cfg.Namespace,
		apiReader:              cfg.APIReader,
		placementLeaseName:     cfg.PlacementLeaseName,
		portal:                 cfg.Portal,
		encryptionKeyRefPrefix: cfg.EncryptPrefix,
		keys:                   cfg.Keys,
		pskReader:              pskReader,
	}
}

func (s *ControllerServer) selectPlacement(ctx context.Context, pool string, requested int64, pinnedOwner, pinnedGUID string, domains ...string) (placement.Candidate, error) {
	return s.selectCapacity(ctx, pool, placement.CapacityRequest{RequestedCapacity: requested}, pinnedOwner, pinnedGUID, domains...)
}

func (s *ControllerServer) selectCapacity(ctx context.Context, pool string, request placement.CapacityRequest, pinnedOwner, pinnedGUID string, domains ...string) (placement.Candidate, error) {
	reader := s.apiReader
	if reader == nil {
		reader = s.client
	}
	nodes := &zfscsiv1.StorageNodeList{}
	if err := reader.List(ctx, nodes); err != nil {
		return placement.Candidate{}, fmt.Errorf("list StorageNodes: %w", err)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := reader.List(ctx, volumes); err != nil {
		return placement.Candidate{}, fmt.Errorf("list Volumes: %w", err)
	}
	return placement.SelectCapacity(nodes.Items, volumes.Items, pool, request, time.Now(), pinnedOwner, pinnedGUID, domains...)
}

func (s *ControllerServer) acquirePlacementLease(ctx context.Context, holder string) (func(context.Context) error, error) {
	name := s.placementLeaseName
	if name == "" {
		name = defaultPlacementLeaseName
	}
	for {
		now := metav1.NowMicro()
		lease := &coordinationv1.Lease{}
		key := apimachinerytypes.NamespacedName{Namespace: s.namespace, Name: name}
		err := s.client.Get(ctx, key, lease)
		if apierrors.IsNotFound(err) {
			lease = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: s.namespace, Name: name}, Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To(holder), AcquireTime: &now, RenewTime: &now, LeaseDurationSeconds: ptr.To[int32](30)}}
			if err := s.client.Create(ctx, lease); err == nil {
				return s.releasePlacementLease(name, holder), nil
			} else if !apierrors.IsAlreadyExists(err) {
				return nil, fmt.Errorf("create placement Lease: %w", err)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get placement Lease: %w", err)
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" || expiredLease(lease, now.Time) {
			before := lease.DeepCopy()
			lease.Spec.HolderIdentity = ptr.To(holder)
			lease.Spec.AcquireTime = &now
			lease.Spec.RenewTime = &now
			lease.Spec.LeaseDurationSeconds = ptr.To[int32](30)
			if err := s.client.Patch(ctx, lease, crclient.MergeFromWithOptions(before, crclient.MergeFromWithOptimisticLock{})); err == nil {
				return s.releasePlacementLease(name, holder), nil
			} else if !apierrors.IsConflict(err) {
				return nil, fmt.Errorf("claim placement Lease: %w", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func expiredLease(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil {
		return true
	}
	return lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second).Before(now)
}

func (s *ControllerServer) releasePlacementLease(name, holder string) func(context.Context) error {
	return func(ctx context.Context) error {
		lease := &coordinationv1.Lease{}
		key := apimachinerytypes.NamespacedName{Namespace: s.namespace, Name: name}
		if err := s.client.Get(ctx, key, lease); err != nil {
			return crclient.IgnoreNotFound(err)
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != holder {
			return nil
		}
		before := lease.DeepCopy()
		lease.Spec.HolderIdentity = ptr.To("")
		return s.client.Patch(ctx, lease, crclient.MergeFromWithOptions(before, crclient.MergeFromWithOptimisticLock{}))
	}
}

// --- CreateVolume ---

// requestedVolume carries the request-derived, comparable attributes of a
// CreateVolume call for idempotency compatibility checks.
type requestedVolume struct {
	pool string
	// capacityRequired and capacityLimit are the RAW CSI CapacityRange bounds of
	// this request, not the driver's aligned provisioning capacity. Idempotency
	// compares an already-persisted capacity against the range a retry asks for,
	// so a retry that asks for less (or for a differently-rounded amount) is a
	// legitimate retry rather than a conflict.
	capacityRequired int64
	capacityLimit    int64
	kind             zfscsiv1.VolumeType
	blockSize        string
	ownerNode        string
	poolGUID         string
	srcSnap          string
	srcVol           string
	nfsCIDRs         []string
	nfsMode          string
	nfsTLS           bool
	nvmeTLS          bool
	networkDomain    string
}

// volumeSpecCompatible returns codes.AlreadyExists when an existing Volume CR is
// incompatible with a same-name CreateVolume request, or nil for a legitimate
// idempotent retry. EncryptionKeyRef is deliberately NOT compared: it is a
// per-request generated ref, so a same-name retry with the same StorageClass
// intent produces a different opaque value that must not be treated as a
// mismatch.
func volumeSpecCompatible(existing *zfscsiv1.VolumeSpec, want requestedVolume) error {
	if existing.Provenance == zfscsiv1.VolumeProvenanceImported {
		return status.Error(codes.AlreadyExists, "volume name is reserved by an imported volume")
	}
	existingNFSCIDRs := existing.NFSExportCIDRs
	existingNFSMode := existing.NFSExportAccessMode
	if existingNFSMode == "" {
		existingNFSMode = defaultNFSExportAccessMode
	}

	switch {
	case existing.OwnerNode != want.ownerNode:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible owner node (existing=%q requested=%q)",
			existing.OwnerNode, want.ownerNode)
	case want.poolGUID != "" && existing.PoolGUID != want.poolGUID:
		return status.Errorf(codes.AlreadyExists, "volume already exists with incompatible pool GUID (existing=%q requested=%q)", existing.PoolGUID, want.poolGUID)
	case want.networkDomain != "" && existing.NetworkDomain != want.networkDomain:
		return status.Errorf(codes.AlreadyExists, "volume already exists with incompatible network domain (existing=%q requested=%q)", existing.NetworkDomain, want.networkDomain)
	case !capacityRangeSatisfied(existing.Capacity, want.capacityRequired, want.capacityLimit):
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with capacity %d incompatible with requested range [%d, %d]",
			existing.Capacity, want.capacityRequired, want.capacityLimit)
	case existing.Pool != want.pool:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible pool (existing=%q requested=%q)",
			existing.Pool, want.pool)
	case existing.Type != want.kind:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible type (existing=%q requested=%q)",
			existing.Type, want.kind)
	case want.kind == zfscsiv1.VolumeTypeBlock && !sameEffectiveBlockSize(existing.VolBlockSize, want.blockSize):
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible block size (existing=%q requested=%q)",
			existing.VolBlockSize, want.blockSize)
	case existing.SourceSnapshotID != want.srcSnap:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible source snapshot (existing=%q requested=%q)",
			existing.SourceSnapshotID, want.srcSnap)
	case existing.SourceVolumeID != want.srcVol:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible source volume (existing=%q requested=%q)",
			existing.SourceVolumeID, want.srcVol)
	case want.kind == zfscsiv1.VolumeTypeFilesystem && !cidrSetsEqual(existingNFSCIDRs, want.nfsCIDRs):
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible NFS export CIDRs (existing=%q requested=%q)",
			existingNFSCIDRs, want.nfsCIDRs)
	case want.kind == zfscsiv1.VolumeTypeFilesystem && existingNFSMode != want.nfsMode:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible NFS export access mode (existing=%q requested=%q)",
			existingNFSMode, want.nfsMode)
	case want.kind == zfscsiv1.VolumeTypeFilesystem && existing.NFSTLSEnabled != want.nfsTLS:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible NFS TLS intent (existing=%t requested=%t)",
			existing.NFSTLSEnabled, want.nfsTLS)
	case want.kind == zfscsiv1.VolumeTypeBlock && existing.NVMeTLSEnabled != want.nvmeTLS:
		return status.Errorf(codes.AlreadyExists,
			"volume already exists with incompatible NVMe TLS intent (existing=%t requested=%t)",
			existing.NVMeTLSEnabled, want.nvmeTLS)
	case want.nvmeTLS && existing.NVMeTLSPSKSecretName != nvmeTLSPSKSecretName(crNameFor(existing.VolName)):
		return status.Error(codes.AlreadyExists, "volume already exists with incompatible NVMe TLS PSK Secret reference")
	default:
		return nil
	}
}

// capacityRangeSatisfied reports whether an already-provisioned capacity is
// compatible with a same-name retry's CSI CapacityRange. CSI requires
// AlreadyExists only when the existing volume is INCOMPATIBLE with the request,
// and a volume is compatible when it is at least required_bytes and, when a
// non-zero limit_bytes is given, no larger than that limit. Comparing against
// the driver's own aligned capacity instead would wrongly reject retries that
// ask for less than the rounded-up capacity actually provisioned, which is
// exactly what the external-provisioner does when it retries a PVC whose
// requested size is not a whole number of volblocksize units.
func capacityRangeSatisfied(existing, required, limit int64) bool {
	if required > 0 && existing < required {
		return false
	}
	if limit > 0 && existing > limit {
		return false
	}

	return true
}

// requireAuthoritativeSourceBlockSize rejects a clone/restore/snapshot whose
// block source carries no recorded volblocksize.
//
// `zfs clone` inherits volblocksize from its origin and cannot change it, so a
// derived zvol's capacity must align to the SOURCE's actual volblocksize. The
// controller never reads ZFS properties — it is a pure CR writer, and the
// backend property is only reachable from the owning storage-agent — so when
// the persisted Volume/Snapshot metadata has no volblocksize the controller has
// no authoritative value. Assuming the modern 16 KiB default would be a guess:
// a legacy zvol created under an inherited default (8 KiB before OpenZFS 2.2,
// or a pool-level override) would get a volsize that ZFS rejects, or silently
// mis-sized capacity accounting. Fail closed with FailedPrecondition instead.
//
// Filesystem sources are unaffected: dataset recordsize places no constraint on
// refquota, so an empty value there is intentional and safe.
func requireAuthoritativeSourceBlockSize(kind zfscsiv1.VolumeType, blockSize, source string) error {
	if kind != zfscsiv1.VolumeTypeBlock || blockSize != "" {
		return nil
	}

	return status.Errorf(codes.FailedPrecondition,
		"%s records no volblocksize: the actual ZFS block size of a block source is not readable from the controller, "+
			"so a derived volume cannot be safely aligned", source)
}

// sameEffectiveBlockSize compares two volblocksize values by the byte size they
// actually resolve to, so equivalent spellings ("16k", "16K", "16384") and a
// legacy empty value that inherits the same default are all compatible. An
// unparseable persisted value can never match a validated request.
func sameEffectiveBlockSize(existing, want string) bool {
	if existing == want {
		return true
	}
	existingBytes, err := zfs.EffectiveBlockSize(existing)
	if err != nil {
		return false
	}
	wantBytes, err := zfs.EffectiveBlockSize(want)
	if err != nil {
		return false
	}

	return existingBytes == wantBytes
}

func cidrSetsEqual(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	aPrefixes, err := nfs.ParseExportCIDRs(a)
	if err != nil {
		return false
	}
	bPrefixes, err := nfs.ParseExportCIDRs(b)
	if err != nil {
		return false
	}
	return slices.Equal(aPrefixes, bPrefixes)
}

func nvmeTLSPSKSecretName(volumeName string) string {
	return nvmeTLSPSKSecretPrefix + volumeName
}

// ensureNVMeTLSPSKSecret creates one immutable controller-named configured PSK.
// AlreadyExists always wins: retries and races re-read but never overwrite bytes.
// No ownerReference is set because Volume is cluster-scoped and Secret is
// namespaced. Cleanup remains deferred until terminal backend destruction can be
// observed by a component with Secret access; this also preserves Retain volumes.
func (s *ControllerServer) ensureNVMeTLSPSKSecret(ctx context.Context, volumeName string) (string, error) {
	name := nvmeTLSPSKSecretName(volumeName)
	key := crclient.ObjectKey{Namespace: s.namespace, Name: name}
	secretReader := s.apiReader
	if secretReader == nil {
		secretReader = s.client
	}
	existing := &corev1.Secret{}
	if err := secretReader.Get(ctx, key, existing); err == nil {
		if err := validateNVMeTLSPSKSecret(existing); err != nil {
			return "", status.Errorf(codes.FailedPrecondition, "existing NVMe TLS PSK Secret is invalid: %v", err)
		}
		return name, nil
	} else if !apierrors.IsNotFound(err) {
		return "", status.Errorf(codes.Internal, "get NVMe TLS PSK Secret: %v", err)
	}
	// A Secret without a persisted Volume can remain after a failed CreateVolume.
	// Reusing this deterministic orphan preserves retry identity and avoids minting
	// additional configured keys. It is never deleted from this path because a
	// concurrent creator may already have made it authoritative.

	interchange, err := psk.Generate(s.pskReader, psk.HMACSHA256)
	if err != nil {
		return "", status.Errorf(codes.Internal, "generate NVMe TLS PSK: %v", err)
	}
	formatted, err := interchange.Format()
	if err != nil {
		return "", status.Errorf(codes.Internal, "format NVMe TLS PSK: %v", err)
	}
	immutable := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.namespace},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: []byte(formatted)},
	}
	if err := s.client.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", status.Errorf(codes.Internal, "create NVMe TLS PSK Secret: %v", err)
		}
		winner := &corev1.Secret{}
		if err := secretReader.Get(ctx, key, winner); err != nil {
			return "", status.Errorf(codes.Internal, "read winning NVMe TLS PSK Secret: %v", err)
		}
		if err := validateNVMeTLSPSKSecret(winner); err != nil {
			return "", status.Errorf(codes.FailedPrecondition, "winning NVMe TLS PSK Secret is invalid: %v", err)
		}
	}
	return name, nil
}

func validateNVMeTLSPSKSecret(secret *corev1.Secret) error {
	if secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable {
		return errors.New("Secret must be immutable and Opaque")
	}
	if len(secret.Data) != 1 {
		return errors.New("Secret must contain only data key psk")
	}
	value, ok := secret.Data[nvmeTLSPSKSecretDataKey]
	if !ok {
		return errors.New("Secret data key psk is missing")
	}
	if _, err := psk.Parse(string(value)); err != nil {
		return errors.New("Secret data key psk is malformed")
	}
	return nil
}

// CreateVolume translates CSI intent into a Volume CR and waits for Ready.
func (s *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}

	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities required")
	}

	sp, err := parseSCParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parameters: %v", err)
	}
	sp, err = applyMutableParams(sp, req.GetMutableParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "mutable parameters: %v", err)
	}

	// Determine kind from SC type or from the first capability.
	kind := volumeKindFromParams(sp)
	// Validate requested capabilities against the kind.
	if err := validateCapabilities(req.GetVolumeCapabilities(), kind); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "capabilities: %v", err)
	}
	// Required bytes. The raw CapacityRange bounds are kept alongside the aligned
	// provisioning capacity: alignment decides what to provision, but idempotency
	// compatibility is judged against the range the caller actually asked for.
	reqBytes := volBytes(req)
	if reqBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity range required")
	}
	requiredBytes := reqBytes
	limitBytes := req.GetCapacityRange().GetLimitBytes()

	zkind, _ := kindToZfs(kind)
	sourceSnapshotID, sourceVolumeID, err := validateVolumeContentSource(req.GetVolumeContentSource(), sp.Pool, zkind)
	if err != nil {
		return nil, err
	}
	var sourceOwner, sourcePoolGUID, sourceDomain string
	// blockSize is the effective volblocksize this volume will actually have. For a
	// fresh block volume parseSCParams has already canonicalised it (defaulting to
	// 16k) so the persisted value is explicit and create/expand alignment agree.
	// `zfs clone` inherits volblocksize from the origin and cannot change it, so a
	// clone/restore must align to the source's block size, not the StorageClass's.
	blockSize := sp.BlockSize
	if sourceVolumeID != "" {
		sourceParsed, _ := naming.ParseVolID(sourceVolumeID)
		sourceVolume := &zfscsiv1.Volume{}
		if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(sourceParsed.ID)}, sourceVolume); err != nil {
			return nil, status.Errorf(codes.NotFound, "source volume CR: %v", err)
		}
		if sourceVolume.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
			return nil, status.Error(codes.FailedPrecondition, "cloning imported volumes is not supported")
		}
		sourceOwner, sourcePoolGUID, sourceDomain = sourceVolume.Spec.OwnerNode, sourceVolume.Spec.PoolGUID, sourceVolume.Spec.NetworkDomain
		blockSize = sourceVolume.Spec.VolBlockSize
		if err := requireAuthoritativeSourceBlockSize(zfscsiv1.VolumeType(kind), blockSize,
			fmt.Sprintf("source volume %s", sourceVolumeID)); err != nil {
			return nil, err
		}
	}
	if sourceSnapshotID != "" {
		_, snapName, _ := naming.ParseSnapID(sourceSnapshotID)
		sourceSnapshot := &zfscsiv1.Snapshot{}
		if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(snapName)}, sourceSnapshot); err != nil {
			return nil, status.Errorf(codes.NotFound, "source snapshot CR: %v", err)
		}
		sourceOwner, sourcePoolGUID = sourceSnapshot.Spec.OwnerNode, sourceSnapshot.Spec.PoolGUID
		// A snapshot restore clones the snapshot's parent dataset, so the parent's
		// volblocksize is what the new zvol will carry. The Snapshot CR records it at
		// snapshot time and it is immutable, so it stays authoritative even when a
		// retained parent's Volume CR has been removed. Older Snapshot CRs lack the
		// field; those fall back to the parent Volume CR, which is equally
		// authoritative. The StorageClass parameter is NOT a fallback here: it
		// describes the requested volume, not the origin whose block size the restore
		// unavoidably inherits, so an unresolvable source stays empty and is rejected
		// below rather than silently aligned to the wrong value. Filesystem restores
		// keep the StorageClass fallback: recordsize constrains no capacity and is a
		// mutable dataset property, so it carries request intent rather than an
		// inherited invariant.
		if zfscsiv1.VolumeType(kind) == zfscsiv1.VolumeTypeBlock {
			blockSize = ""
		}
		switch ref := sourceSnapshot.Spec.VolumeRef; {
		case sourceSnapshot.Spec.SourceVolBlockSize != "":
			blockSize = sourceSnapshot.Spec.SourceVolBlockSize
		case ref != "":
			parent := &zfscsiv1.Volume{}
			switch err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: ref}, parent); {
			case err == nil:
				blockSize = parent.Spec.VolBlockSize
			case !apierrors.IsNotFound(err):
				return nil, status.Errorf(codes.Internal, "get source snapshot parent volume CR: %v", err)
			}
		}
		if err := requireAuthoritativeSourceBlockSize(zfscsiv1.VolumeType(kind), blockSize,
			fmt.Sprintf("source snapshot %s", sourceSnapshotID)); err != nil {
			return nil, err
		}
	}

	// Capacity alignment must happen before placement reservation and before the
	// Volume CR is written, so reservations and spec.capacity both carry the
	// capacity ZFS will actually provision.
	reqBytes, err = alignedCapacity(requiredBytes, limitBytes, zfscsiv1.VolumeType(kind), blockSize)
	if err != nil {
		return nil, err
	}

	// Derive volume id + CR name. The volID encodes the sanitised leaf id;
	// the CR name is derived from the SAME sanitised id so that consumers
	// (which only have the volID) can round-trip to the CR name (Publish/
	// Expand/Delete all parse the volID and derive crName from its id part).
	leafID := sanitizeID(req.GetName())
	volID := s.volumeID(sp.Pool, zkind, req.GetName())
	crName := crNameFor(leafID)

	want := requestedVolume{
		pool:             sp.Pool,
		capacityRequired: requiredBytes,
		capacityLimit:    limitBytes,
		kind:             zfscsiv1.VolumeType(kind),
		blockSize:        blockSize,
		srcSnap:          sourceSnapshotID,
		srcVol:           sourceVolumeID,
		nfsCIDRs:         sp.NFSExportCIDRs,
		nfsMode:          sp.NFSExportAccessMode,
		nfsTLS:           sp.NFSTLSEnabled,
		nvmeTLS:          sp.NVMeTLSEnabled,
	}
	if sourceOwner != "" {
		want.ownerNode, want.poolGUID, want.networkDomain = sourceOwner, sourcePoolGUID, sourceDomain
	}

	// Idempotent retry: do not mint another DEK when the Volume CR already
	// exists, because key-provider refs are opaque and may map to shared state.
	// CSI requires AlreadyExists when a same-name request is INCOMPATIBLE with
	// the existing volume; a compatible request is a legitimate idempotent retry.
	existing := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.Internal, "get Volume CR before create: %v", err)
		}
	} else {
		// Existing same-name retries are bound by persisted owner and pool identity;
		// inventory availability must not break CSI idempotency.
		want.ownerNode = existing.Spec.OwnerNode
		want.poolGUID = ""
		if err := volumeSpecCompatible(&existing.Spec, want); err != nil {
			return nil, err
		}

		return s.readyVolumeResponse(ctx, crName, volID, req)
	}

	// Key generation may block on the external provider and must never consume
	// the short placement Lease. The initial same-name lookup above preserves
	// idempotent retries. OpenBao Generate uses a deterministic Transit key name;
	// its returned ciphertext is not server-persisted, so a losing create race
	// must not delete that shared key because the winner may already reference it.
	encRef := ""
	if sp.Encrypted {
		if s.keys == nil {
			return nil, status.Error(codes.FailedPrecondition, "encrypted volume requested but key provider is not configured")
		}
		encRef, err = s.keys.Generate(ctx, shortHash(req.GetName()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate encryption key: %v", err)
		}
	}
	pskSecretName := ""
	if sp.NVMeTLSEnabled {
		pskSecretName, err = s.ensureNVMeTLSPSKSecret(ctx, crName)
		if err != nil {
			return nil, err
		}
	}

	holder := leafID + "-" + shortHash(req.GetName()+time.Now().UTC().String())
	releasePlacement, lockErr := s.acquirePlacementLease(ctx, holder)
	if lockErr != nil {
		return nil, waitStatusError("acquire placement reservation", lockErr)
	}
	placementReleased := false
	defer func() {
		if !placementReleased {
			_ = releasePlacement(context.WithoutCancel(ctx))
		}
	}()
	// Re-read under the cluster-wide placement lock: another controller may
	// have created the same idempotency key while this request waited.
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, existing); err == nil {
		want.ownerNode, want.poolGUID = existing.Spec.OwnerNode, existing.Spec.PoolGUID
		if err := volumeSpecCompatible(&existing.Spec, want); err != nil {
			return nil, err
		}
		if err := releasePlacement(context.WithoutCancel(ctx)); err != nil {
			return nil, status.Errorf(codes.Internal, "release placement Lease: %v", err)
		}
		placementReleased = true
		return s.readyVolumeResponse(ctx, crName, volID, req)
	} else if !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "re-read Volume CR before placement: %v", err)
	}
	domainOrder, constrained, topologyErr := topologyDomains(req.GetAccessibilityRequirements())
	if topologyErr != nil {
		return nil, topologyErr
	}
	if want.networkDomain != "" {
		domainOrder, constrained = []string{want.networkDomain}, true
	}
	candidate, placeErr := s.selectPlacement(ctx, want.pool, reqBytes, want.ownerNode, want.poolGUID, domainOrder...)
	if placeErr != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "place volume: %v", placeErr)
	}
	if constrained && candidate.NetworkDomain == "" {
		return nil, status.Error(codes.ResourceExhausted, "place volume: no candidate reachable from requisite topology")
	}
	want.ownerNode, want.poolGUID, want.networkDomain = candidate.OwnerNode, candidate.PoolGUID, candidate.NetworkDomain

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       crName,
			Finalizers: []string{zfscsiv1.VolumeFinalizer},
		},
		Spec: zfscsiv1.VolumeSpec{
			Pool:                 sp.Pool,
			Capacity:             reqBytes,
			Type:                 zfscsiv1.VolumeType(kind),
			FsType:               sp.FsType,
			VolBlockSize:         blockSize,
			Compression:          sp.Compression,
			EncryptionKeyRef:     encRef,
			Transport:            zfscsiv1.TransportKind(sp.Transport),
			OwnerNode:            want.ownerNode,
			PoolGUID:             want.poolGUID,
			NetworkDomain:        want.networkDomain,
			VolName:              req.GetName(),
			VolumeID:             volID,
			SourceSnapshotID:     sourceSnapshotID,
			SourceVolumeID:       sourceVolumeID,
			NFSExportCIDRs:       sp.NFSExportCIDRs,
			NFSExportAccessMode:  sp.NFSExportAccessMode,
			NFSTLSEnabled:        sp.NFSTLSEnabled,
			NVMeTLSEnabled:       sp.NVMeTLSEnabled,
			NVMeTLSPSKSecretName: pskSecretName,
		},
	}

	// Idempotent create: if it exists, reuse it.
	createLog := logging.LogWith(s.log, logging.OpCreateVolumeCR, logging.KeyVolumeID, volID, logging.KeyCRName, crName, logging.KeyCapacity, reqBytes)
	if err := s.client.Create(ctx, vol); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			createLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "create Volume CR: %v", err)
		}

		// A concurrent creator won the race. Re-read and enforce the same
		// compatibility gate: an incompatible same-name volume must AlreadyExists.
		raced := &zfscsiv1.Volume{}
		if getErr := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, raced); getErr == nil {
			if incErr := volumeSpecCompatible(&raced.Spec, want); incErr != nil {
				return nil, incErr
			}
		}

		createLog.With("alreadyExists", true).OK()
	} else {
		createLog.OK()
	}
	if err := releasePlacement(context.WithoutCancel(ctx)); err != nil {
		return nil, status.Errorf(codes.Internal, "release placement Lease: %v", err)
	}
	placementReleased = true

	// Wait for the agent to report Ready (bounded). This is the CR contract.
	if err := s.waitForReady(ctx, crName); err != nil {
		if errors.Is(err, errVolumeInErrorState) {
			return nil, status.Errorf(codes.Internal, "volume %s in error state: %v", volID, err)
		}

		return nil, waitStatusError(fmt.Sprintf("volume %s not ready", volID), err)
	}

	return s.volumeResponse(ctx, crName, volID, req)
}

func topologyDomains(requirement *csi.TopologyRequirement) ([]string, bool, error) {
	if requirement == nil {
		return nil, false, nil
	}
	if len(requirement.GetRequisite()) == 0 && len(requirement.GetPreferred()) == 0 {
		return nil, false, nil
	}
	extract := func(topologies []*csi.Topology) ([]string, error) {
		result := make([]string, 0, len(topologies))
		for _, topology := range topologies {
			if topology == nil {
				continue
			}
			domain, ok := topology.GetSegments()[reachability.TopologyKeyNetworkDomain]
			if !ok || domain == "" {
				return nil, status.Errorf(codes.InvalidArgument, "topology must contain %s", reachability.TopologyKeyNetworkDomain)
			}
			if !slices.Contains(result, domain) {
				result = append(result, domain)
			}
		}
		return result, nil
	}
	requisite, err := extract(requirement.GetRequisite())
	if err != nil {
		return nil, false, err
	}
	preferred, err := extract(requirement.GetPreferred())
	if err != nil {
		return nil, false, err
	}
	order, constrained := reachability.DomainOrder(requisite, preferred)
	return order, constrained, nil
}

func (s *ControllerServer) readyVolumeResponse(ctx context.Context, crName, volID string, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := s.waitForReady(ctx, crName); err != nil {
		if errors.Is(err, errVolumeInErrorState) {
			return nil, status.Errorf(codes.Internal, "volume %s in error state: %v", volID, err)
		}

		return nil, waitStatusError(fmt.Sprintf("volume %s not ready", volID), err)
	}

	return s.volumeResponse(ctx, crName, volID, req)
}

func (s *ControllerServer) volumeResponse(ctx context.Context, crName, volID string, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Read the materialised status for the transport handle.
	got := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, got); err != nil {
		return nil, status.Errorf(codes.Internal, "get Volume CR after ready: %v", err)
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volID,
			CapacityBytes:      got.Status.ActualCapacity,
			AccessibleTopology: s.topology(got),
			ContentSource:      volumeContentSource(req),
			VolumeContext:      volumeContextForVolume(got),
		},
	}

	return resp, nil
}

// --- DeleteVolume ---

// DeleteVolume marks the Volume CR for deletion. The agent keeps its finalizer
// until backend teardown, DEK crypto-shred, and PSK Secret cleanup finish.
func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}

	p, ok := parseVolumeID(req.GetVolumeId())
	if !ok {
		// Not ours → nothing to do (could be an adopted/migrated volume).
		return &csi.DeleteVolumeResponse{}, nil
	}

	crName := crNameFor(p.ID)

	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return &csi.DeleteVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "get Volume CR for delete: %v", err)
	}
	// In-use guard (F6): refuse to delete a volume that is still published to a
	// node. Normal CSI flow never hits this — PVC-protection + the external
	// attacher guarantee ControllerUnpublish precedes DeleteVolume — so a
	// non-empty mappedInitiators here means a stale VolumeAttachment or an
	// out-of-band/operator delete while a consumer still holds the device.
	// Proceeding would rip the target out from under live I/O and zfs-destroy the
	// data. The operator escape is the force-delete annotation (known-stale
	// mapping). mappedInitiators (controller-owned) is the authoritative gate;
	// publishedInitiators is agent-owned and can lag.
	if len(vol.Status.MappedInitiators) > 0 && !forceDeleteRequested(vol) {
		nodes := make([]string, 0, len(vol.Status.MappedInitiators))
		for _, m := range vol.Status.MappedInitiators {
			nodes = append(nodes, m.NodeName)
		}

		return nil, status.Errorf(codes.FailedPrecondition,
			"volume %s still published to node(s) %v; unpublish first or set annotation %s=true to force",
			req.GetVolumeId(), nodes, zfscsiv1.ForceDeleteAnnotation)
	}

	// Ensure deletion is held until the agent finishes dataset/export/DEK cleanup.
	patch := crclient.MergeFrom(vol.DeepCopy())
	ensureFinalizer(&vol.Finalizers, zfscsiv1.VolumeFinalizer)
	if err := s.client.Patch(ctx, vol, patch); err != nil {
		return nil, status.Errorf(codes.Internal, "patch Volume CR finalizer: %v", err)
	}

	// Best-effort: delete the CR so the agent's destroy + finalizer runs. The
	// agent's finalizer completes the destroy then removes itself.
	deleteLog := logging.LogWith(s.log, logging.OpDeleteVolumeCR, logging.KeyVolumeID, req.GetVolumeId(), logging.KeyCRName, crName)
	if err := s.client.Delete(ctx, vol); err != nil {
		if !apierrors.IsNotFound(err) {
			deleteLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "delete Volume CR: %v", err)
		}

		deleteLog.With("notFound", true).OK()
	} else {
		deleteLog.OK()
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// forceDeleteRequested reports whether the operator set the force-delete
// annotation to "true", overriding the in-use deletion guard.
func forceDeleteRequested(vol *zfscsiv1.Volume) bool {
	return vol.Annotations[zfscsiv1.ForceDeleteAnnotation] == "true"
}

// --- ControllerPublish / Unpublish (initiator map management) ---

// ControllerPublishVolume maps the consumer node's initiator onto the volume's
// block target (server-side configfs write by the agent; here we record intent
// in the CR status). For filesystem volumes this is a no-op (NFS is stateless).
func (s *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id + node_id required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse volume id: %v", err)
	}

	crName := crNameFor(p.ID)

	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume CR: %v", err)
	}

	if vol.Status.CurrentState() != zfscsiv1.VolumeStateReady && vol.Status.CurrentState() != zfscsiv1.VolumeStateReadyToPublish {
		return nil, status.Errorf(codes.FailedPrecondition, "volume not ready (state=%s)", vol.Status.CurrentState())
	}
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported && vol.Status.ObservedGeneration != vol.Generation {
		return nil, status.Error(codes.FailedPrecondition, "imported volume validation is stale")
	}

	// Filesystem volumes: no publish step.
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		publishContext := publishContextForVolume(vol)
		if publishContext == nil {
			return nil, status.Error(codes.FailedPrecondition, "volume NFS endpoint is not materialized")
		}
		return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
	}

	// Block: record the initiator mapping. The actual configfs allow-host write
	// is done by the storage-agent reconciler observing this status field.
	initiatorID := req.GetNodeId()
	volID, nodeID := req.GetVolumeId(), req.GetNodeId()

	mapped := zfscsiv1.MappedInitiator{
		NodeName:    nodeID,
		InitiatorID: initiatorID,
		AttachedAt:  metav1.Now(),
	}
	// Controller owns ONLY mappedInitiators. Agent owns targetNQN/portal/etc.
	// Optimistic lock ensures we retry on 409 if another writer changed the
	// resourceVersion between our Get and Patch.
	patchLog := logging.LogWith(s.log, logging.OpPatchVolumeStatus, logging.KeyVolumeID, volID, logging.KeyCRName, crName, logging.KeyInitiator, initiatorID)
	if err := s.patchMappedInitiatorWithRetry(ctx, vol, mapped, req.GetVolumeCapability()); err != nil {
		patchLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "patch initiator map: %v", err)
	}
	patchLog.OK()

	if err := s.waitForPublishedInitiator(ctx, crName, initiatorID); err != nil {
		return nil, waitStatusError("wait for confirmed initiator mapping", err)
	}

	// Re-read the materialised status for the transport handle.
	got := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, got); err != nil {
		return nil, status.Errorf(codes.Internal, "get Volume CR after publish: %v", err)
	}

	publishContext := publishContextForVolume(got)
	if publishContext == nil {
		return nil, status.Error(codes.FailedPrecondition, "volume transport identity is not materialized")
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
}

// ControllerUnpublishVolume removes the initiator mapping.
func (s *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse volume id: %v", err)
	}

	crName := crNameFor(p.ID)

	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "get Volume CR: %v", err)
	}

	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	nodeID := req.GetNodeId()
	if err := s.removeMappedInitiatorWithRetry(ctx, vol, nodeID); err != nil {
		logging.LogWith(s.log, logging.OpPatchVolumeStatus, logging.KeyVolumeID, req.GetVolumeId(), logging.KeyCRName, crName, logging.KeyInitiator, nodeID).Failed(err)

		return nil, status.Errorf(codes.Internal, "patch initiator unmap: %v", err)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// removeMappedInitiatorWithRetry removes a single node's entry from
// status.mappedInitiators under an optimistic-lock precondition, re-reading on
// 409 conflict. A plain merge patch replaces the whole list wholesale, so an
// unpublish interleaving with a concurrent single-writer publish for another
// node would clobber the publish's freshly-written list (lost update). The
// optimistic lock forces the unpublish to re-read the current list each attempt
// and remove only this node's entry, so a concurrent publish survives.
func (s *ControllerServer) removeMappedInitiatorWithRetry(ctx context.Context, vol *zfscsiv1.Volume, nodeID string) error {
	const maxRetries = 5

	for range maxRetries {
		// Capture baseline BEFORE mutation with an optimistic-lock precondition.
		patch := crclient.MergeFromWithOptions(vol.DeepCopy(), crclient.MergeFromWithOptimisticLock{})
		vol.Status.MappedInitiators = removeInitiator(vol.Status.MappedInitiators, nodeID)

		if err := s.client.Status().Patch(ctx, vol, patch); err != nil {
			if apierrors.IsConflict(err) {
				if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: vol.Name}, vol); err != nil {
					return fmt.Errorf("re-get on conflict: %w", err)
				}

				continue
			}

			return err
		}

		return nil
	}

	return errors.New("patch mappedInitiators unmap: exceeded retry limit")
}

// --- ControllerExpandVolume ---

// ControllerExpandVolume grows the Volume CR capacity (agent applies zfs set).
func (s *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse volume id: %v", err)
	}

	crName := crNameFor(p.ID)

	key := apimachinerytypes.NamespacedName{Name: crName}
	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, key, vol); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume CR: %v", err)
	}

	if err := validateExpansionIdentity(vol, req.GetVolumeId(), p); err != nil {
		return nil, err
	}

	// Align against the volume's own effective volblocksize (persisted at
	// creation, immutable thereafter), not a StorageClass parameter: expansion
	// carries no parameters and `zfs set volsize` has the same alignment rule as
	// create.
	cap, err := alignedCapacity(
		req.GetCapacityRange().GetRequiredBytes(),
		req.GetCapacityRange().GetLimitBytes(),
		vol.Spec.Type,
		vol.Spec.VolBlockSize,
	)
	if err != nil {
		return nil, err
	}
	if cap <= vol.Spec.Capacity {
		return expandVolumeResponse(vol, vol.Spec.Capacity), nil
	}

	holder := "expand-" + crName + "-" + shortHash(req.GetVolumeId()+time.Now().UTC().String())
	releasePlacement, lockErr := s.acquirePlacementLease(ctx, holder)
	if lockErr != nil {
		return nil, waitStatusError("acquire placement reservation", lockErr)
	}
	placementReleased := false
	defer func() {
		if !placementReleased {
			_ = releasePlacement(context.WithoutCancel(ctx))
		}
	}()

	reader := s.apiReader
	if reader == nil {
		reader = s.client
	}
	fresh := &zfscsiv1.Volume{}
	if err := reader.Get(ctx, key, fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume CR: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "re-read Volume CR before expansion: %v", err)
	}
	if err := validateExpansionIdentity(fresh, req.GetVolumeId(), p); err != nil {
		return nil, err
	}
	if cap <= fresh.Spec.Capacity {
		if err := releasePlacement(context.WithoutCancel(ctx)); err != nil {
			return nil, status.Errorf(codes.Internal, "release placement Lease: %v", err)
		}
		placementReleased = true
		return expandVolumeResponse(fresh, fresh.Spec.Capacity), nil
	}
	if _, err := s.selectCapacity(ctx, fresh.Spec.Pool, placement.CapacityRequest{RequestedCapacity: cap, ExistingCapacity: fresh.Spec.Capacity}, fresh.Spec.OwnerNode, fresh.Spec.PoolGUID); err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "expand volume on pinned pool: %v", err)
	}

	// Clear the sample marker first. From this point until a post-growth pool
	// sample, placement reserves the full spec capacity, including on retries.
	if err := s.clearCapacityAccountedAt(ctx, key); err != nil {
		return nil, status.Errorf(codes.Internal, "clear Volume capacity accounting before expansion: %v", err)
	}
	if err := reader.Get(ctx, key, fresh); err != nil {
		return nil, status.Errorf(codes.Internal, "re-read Volume CR after capacity accounting reset: %v", err)
	}
	if err := validateExpansionIdentity(fresh, req.GetVolumeId(), p); err != nil {
		return nil, err
	}
	if cap <= fresh.Spec.Capacity {
		if err := releasePlacement(context.WithoutCancel(ctx)); err != nil {
			return nil, status.Errorf(codes.Internal, "release placement Lease: %v", err)
		}
		placementReleased = true
		return expandVolumeResponse(fresh, fresh.Spec.Capacity), nil
	}

	patch := crclient.MergeFromWithOptions(fresh.DeepCopy(), crclient.MergeFromWithOptimisticLock{})
	fresh.Spec.Capacity = cap
	patchLog := logging.LogWith(s.log, logging.OpPatchVolumeCR, logging.KeyVolumeID, req.GetVolumeId(), logging.KeyCRName, crName, logging.KeyCapacity, cap)
	if err := s.client.Patch(ctx, fresh, patch); err != nil {
		patchLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "patch Volume CR capacity: %v", err)
	}
	patchLog.OK()
	if err := releasePlacement(context.WithoutCancel(ctx)); err != nil {
		return nil, status.Errorf(codes.Internal, "release placement Lease: %v", err)
	}
	placementReleased = true
	return expandVolumeResponse(fresh, cap), nil
}

func validateExpansionIdentity(vol *zfscsiv1.Volume, volumeID string, parsed naming.ParsedVolID) error {
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		return status.Error(codes.FailedPrecondition, "expansion is not supported for imported volumes")
	}
	if vol.Spec.VolumeID != volumeID || vol.Spec.Pool != parsed.Pool || string(vol.Spec.Type) != string(parsed.Kind) {
		return status.Error(codes.FailedPrecondition, "volume identity no longer matches requested volume_id")
	}
	if vol.Spec.OwnerNode == "" || vol.Spec.PoolGUID == "" {
		return status.Error(codes.FailedPrecondition, "volume has no pinned placement identity")
	}
	return nil
}

func expandVolumeResponse(vol *zfscsiv1.Volume, capacity int64) *csi.ControllerExpandVolumeResponse {
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: capacity, NodeExpansionRequired: vol.Spec.Type == zfscsiv1.VolumeTypeBlock}
}

func (s *ControllerServer) clearCapacityAccountedAt(ctx context.Context, key apimachinerytypes.NamespacedName) error {
	reader := s.apiReader
	if reader == nil {
		reader = s.client
	}
	return wait.ExponentialBackoffWithContext(ctx, wait.Backoff{Duration: 10 * time.Millisecond, Factor: 2, Steps: 6, Cap: 200 * time.Millisecond}, func(ctx context.Context) (bool, error) {
		vol := &zfscsiv1.Volume{}
		if err := reader.Get(ctx, key, vol); err != nil {
			return false, err
		}
		if vol.Status.CapacityAccountedAt == nil {
			return true, nil
		}
		before := vol.DeepCopy()
		vol.Status.CapacityAccountedAt = nil
		if err := s.client.Status().Patch(ctx, vol, crclient.MergeFromWithOptions(before, crclient.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

// --- Snapshots ---

// CreateSnapshot writes a Snapshot CR and waits for ReadyToUse.
func (s *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetName() == "" || req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name + source_volume_id required")
	}

	p, err := naming.ParseVolID(req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse source volume id: %v", err)
	}
	source := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(p.ID)}, source); err != nil {
		return nil, status.Errorf(codes.NotFound, "source volume CR: %v", err)
	}
	if source.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		return nil, status.Error(codes.FailedPrecondition, "snapshots are not supported for imported volumes")
	}
	if source.Spec.OwnerNode == "" {
		return nil, status.Error(codes.FailedPrecondition, "source volume owner is unavailable")
	}
	// A restore clones this snapshot and inherits the source's volblocksize, so the
	// recorded value must be authoritative. Recording an empty value for a block
	// source would bake in a guess that later restores would align to.
	if err := requireAuthoritativeSourceBlockSize(source.Spec.Type, source.Spec.VolBlockSize,
		fmt.Sprintf("source volume %s", req.GetSourceVolumeId())); err != nil {
		return nil, err
	}

	snapID, err := naming.EncodeSnapID(p.Pool, p.Kind, p.ID, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "encode snap id: %v", err)
	}

	srcCRName := crNameFor(p.ID)

	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       crNameFor(req.GetName()),
			Finalizers: []string{zfscsiv1.SnapshotFinalizer},
		},
		Spec: zfscsiv1.SnapshotSpec{
			VolumeRef:      srcCRName,
			SourceVolumeID: req.GetSourceVolumeId(),
			SnapName:       req.GetName(),
			SnapshotID:     snapID,
			OwnerNode:      source.Spec.OwnerNode,
			PoolGUID:       source.Spec.PoolGUID,
			// Recorded so a restore can align capacity to the block size it will
			// inherit even after a retained parent's Volume CR is gone.
			SourceVolBlockSize: source.Spec.VolBlockSize,
		},
	}
	createLog := logging.LogWith(s.log, logging.OpCreateSnapshotCR,
		logging.KeySnapshotID, snapID,
		logging.KeyCRName, crNameFor(req.GetName()),
		logging.KeySourceVolumeID, req.GetSourceVolumeId())
	if err := s.client.Create(ctx, snap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			createLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "create Snapshot CR: %v", err)
		}

		// CSI requires AlreadyExists when a same-name snapshot exists for a
		// DIFFERENT source volume; a same-source retry is idempotent.
		existing := &zfscsiv1.Snapshot{}
		if getErr := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(req.GetName())}, existing); getErr == nil {
			if existing.Spec.SourceVolumeID != req.GetSourceVolumeId() || existing.Spec.OwnerNode != source.Spec.OwnerNode || existing.Spec.PoolGUID != source.Spec.PoolGUID {
				return nil, status.Errorf(codes.AlreadyExists,
					"snapshot %q already exists for a different source volume (existing=%q requested=%q)",
					req.GetName(), existing.Spec.SourceVolumeID, req.GetSourceVolumeId())
			}
		}

		createLog.With("alreadyExists", true).OK()
	} else {
		createLog.OK()
	}

	if err := s.waitForSnapshotReady(ctx, crNameFor(req.GetName())); err != nil {
		if errors.Is(err, errSnapshotInErrorState) {
			return nil, status.Errorf(codes.Internal, "snapshot in error state: %v", err)
		}

		return nil, waitStatusError("snapshot not ready", err)
	}

	got := &zfscsiv1.Snapshot{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(req.GetName())}, got); err != nil {
		return nil, status.Errorf(codes.Internal, "get Snapshot CR: %v", err)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapID,
			SourceVolumeId: req.GetSourceVolumeId(),
			SizeBytes:      got.Status.SizeBytes(),
			CreationTime:   timestampp(got.Status.CreatedAtUnix()),
			ReadyToUse:     got.Status.Ready(),
		},
	}, nil
}

// DeleteSnapshot marks the Snapshot CR Deleting.
func (s *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id required")
	}

	_, snapName, ok := parseSnapshotID(req.GetSnapshotId())
	if !ok {
		return &csi.DeleteSnapshotResponse{}, nil // not ours
	}

	crName := crNameFor(snapName)
	snap := &zfscsiv1.Snapshot{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return &csi.DeleteSnapshotResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal, "get Snapshot CR for delete: %v", err)
	}

	patch := crclient.MergeFrom(snap.DeepCopy())
	ensureFinalizer(&snap.Finalizers, zfscsiv1.SnapshotFinalizer)
	if err := s.client.Patch(ctx, snap, patch); err != nil {
		return nil, status.Errorf(codes.Internal, "patch Snapshot CR finalizer: %v", err)
	}

	deleteLog := logging.LogWith(s.log, logging.OpDeleteSnapshotCR, logging.KeySnapshotID, req.GetSnapshotId(), logging.KeyCRName, crName)
	if err := s.client.Delete(ctx, snap); err != nil &&
		!apierrors.IsNotFound(err) {
		deleteLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "delete Snapshot CR: %v", err)
	} else if err != nil {
		deleteLog.With("notFound", true).OK()
	} else {
		deleteLog.OK()
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// --- simple RPCs ---

func (s *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" || len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_id + capabilities required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "unknown volume id: %v", err)
	}

	kind := map[string]string{"block": "block", "filesystem": "filesystem"}[string(p.Kind)]

	sp := scParams{Type: kind}
	if message := validateCapabilitiesMessage(req.GetVolumeCapabilities(), volumeKindFromParams(sp)); message != "" {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: message}, nil
	}

	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: req.GetVolumeCapabilities(),
	}}, nil
}

func (s *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	list := &zfscsiv1.VolumeList{}
	// Volumes are cluster-scoped and their names are globally unique CSI identities.
	if err := s.client.List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(list.Items))
	for i := range list.Items {
		v := &list.Items[i]
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      v.Spec.VolumeID,
				CapacityBytes: v.Status.ActualCapacity,
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				VolumeCondition: volumeConditionFor(v),
			},
		})
	}

	return &csi.ListVolumesResponse{Entries: entries}, nil
}

func (s *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	params, err := parseSCParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parameters: %v", err)
	}
	requestedDomains := []string(nil)
	if topology := req.GetAccessibleTopology(); topology != nil {
		domain, ok := topology.GetSegments()[reachability.TopologyKeyNetworkDomain]
		if !ok || domain == "" {
			return nil, status.Errorf(codes.InvalidArgument, "topology must contain %s", reachability.TopologyKeyNetworkDomain)
		}
		requestedDomains = []string{domain}
	}
	reader := s.apiReader
	if reader == nil {
		reader = s.client
	}
	nodes := &zfscsiv1.StorageNodeList{}
	if err := reader.List(ctx, nodes); err != nil {
		return nil, status.Errorf(codes.Unavailable, "storage inventory unavailable: %v", err)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := reader.List(ctx, volumes); err != nil {
		return nil, status.Errorf(codes.Unavailable, "volume reservations unavailable: %v", err)
	}
	if candidate, err := placement.Select(nodes.Items, volumes.Items, params.Pool, 0, time.Now(), "", "", requestedDomains...); err == nil {
		return &csi.GetCapacityResponse{AvailableCapacity: candidate.Available}, nil
	} else if len(requestedDomains) > 0 {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}
	// Each storage agent publishes its own per-node ConfigMap; aggregate across
	// all of them. For a network-attached pool reachable from any node we report
	// the largest free-bytes value observed for the requested pool (a single
	// pool published by one owner node; the max is that node's value and is
	// robust to a stale duplicate). No matching data → Unavailable so external-
	// provisioner retries until the agent publishes.
	list := &corev1.ConfigMapList{}
	if err := s.client.List(ctx, list,
		crclient.InNamespace(s.namespace),
		crclient.MatchingLabels{capacity.ManagedByLabel: capacity.ManagedByValue},
	); err != nil {
		return nil, status.Errorf(codes.Unavailable, "capacity data unavailable: %v", err)
	}
	var (
		available int64 = -1
	)
	for i := range list.Items {
		raw, ok := list.Items[i].Data[params.Pool]
		if !ok {
			continue
		}
		free, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || free < 0 {
			continue
		}
		if free > available {
			available = free
		}
	}
	if available < 0 {
		return nil, status.Errorf(codes.Unavailable, "capacity for pool %q unavailable", params.Pool)
	}
	return &csi.GetCapacityResponse{AvailableCapacity: available}, nil
}

func (s *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: controllerCapabilities()}, nil
}

func (s *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	list := &zfscsiv1.SnapshotList{}
	// Snapshots are cluster-scoped and their names are globally unique CSI identities.
	if err := s.client.List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list snapshots: %v", err)
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(list.Items))
	for i := range list.Items {
		s := &list.Items[i]
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     s.Spec.SnapshotID,
				SourceVolumeId: s.Spec.SourceVolumeID,
				SizeBytes:      s.Status.SizeBytes(),
				ReadyToUse:     s.Status.Ready(),
				CreationTime:   timestampp(s.Status.CreatedAtUnix()),
			},
		})
	}

	return &csi.ListSnapshotsResponse{Entries: entries}, nil
}

func (s *ControllerServer) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	_, snapName, err := naming.ParseSnapID(req.GetSnapshotId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "unknown snapshot id: %v", err)
	}

	got := &zfscsiv1.Snapshot{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(snapName)}, got); err != nil {
		return nil, status.Errorf(codes.NotFound, "snapshot CR: %v", err)
	}

	return &csi.GetSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     got.Spec.SnapshotID,
			SourceVolumeId: got.Spec.SourceVolumeID,
			SizeBytes:      got.Status.SizeBytes(),
			ReadyToUse:     got.Status.Ready(),
			CreationTime:   timestampp(got.Status.CreatedAtUnix()),
		},
	}, nil
}

func (s *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "unknown volume id: %v", err)
	}

	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crNameFor(p.ID)}, vol); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume CR: %v", err)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      vol.Spec.VolumeID,
			CapacityBytes: vol.Status.ActualCapacity,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: volumeConditionFor(vol),
		},
	}, nil
}

// volumeConditionFor derives CSI health from persisted status. Backend health
// takes precedence because the agent writes it before retrying a repair.
func volumeConditionFor(vol *zfscsiv1.Volume) *csi.VolumeCondition {
	for _, c := range vol.Status.Conditions {
		if c.Type == string(zfscsiv1.VolumeConditionBackendHealthy) && c.Status != metav1.ConditionTrue {
			msg := c.Message
			if msg == "" {
				msg = "volume backend is unhealthy"
			}

			return &csi.VolumeCondition{Abnormal: true, Message: msg}
		}
	}

	switch vol.Status.State {
	case zfscsiv1.VolumeStateReady, zfscsiv1.VolumeStateReadyToPublish:
		return &csi.VolumeCondition{Abnormal: false, Message: "Ready"}
	case zfscsiv1.VolumeStateError:
		msg := "volume in Error state"
		for _, c := range vol.Status.Conditions {
			if c.Type == string(zfscsiv1.VolumeConditionReady) && c.Message != "" {
				msg = c.Message

				break
			}
		}

		return &csi.VolumeCondition{Abnormal: true, Message: msg}
	default:
		return &csi.VolumeCondition{Abnormal: true, Message: "not ready: " + string(vol.Status.State)}
	}
}

// ControllerModifyVolume applies a VolumeAttributesClass's sole supported mutable
// parameter, compression, to an existing volume. The parameter is patched onto the
// Volume CR's spec and the storage agent's level-triggered reconcile applies zfs
// set without recreation or remounting.
func (s *ControllerServer) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse volume id: %v", err)
	}

	if err := validateMutableParams(req.GetMutableParameters()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	compression, err := mutableCompression(req.GetMutableParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if compression == "" {
		// An empty parameter map carries no requested change.
		return &csi.ControllerModifyVolumeResponse{}, nil
	}

	crName := crNameFor(p.ID)
	vol := &zfscsiv1.Volume{}
	if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume CR: %v", err)
	}
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		return nil, status.Error(codes.FailedPrecondition, "mutable volume attributes are not supported for imported volumes")
	}

	if vol.Spec.Compression == compression {
		return &csi.ControllerModifyVolumeResponse{}, nil // already at target
	}

	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Spec.Compression = compression
	patchLog := logging.LogWith(s.log, logging.OpPatchVolumeCR,
		logging.KeyVolumeID, req.GetVolumeId(), logging.KeyCRName, crName, logging.KeyCompression, compression)
	if err := s.client.Patch(ctx, vol, patch); err != nil {
		patchLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "patch Volume CR compression: %v", err)
	}
	patchLog.OK()

	return &csi.ControllerModifyVolumeResponse{}, nil
}

// --- helpers ---

var (
	errVolumeInErrorState    = errors.New("volume in Error state")
	errSnapshotInErrorState  = errors.New("snapshot in Error state")
	errNilCapability         = errors.New("nil capability")
	errUnsupportedAccessMode = errors.New("unsupported access mode")
	errBlockOnFilesystem     = errors.New("raw block volumeMode is not supported by filesystem (NFS) volumes")
)

// kindToZfs maps the SC type string to a zfs.VolumeKind.
func kindToZfs(kind string) (zfs.VolumeKind, error) {
	switch kind {
	case "block":
		return zfs.KindBlock, nil
	case "filesystem":
		return zfs.KindFilesystem, nil
	}

	return "", fmt.Errorf("%w: %q", naming.ErrUnknownKind, kind)
}

func (s *ControllerServer) volumeID(pool string, kind zfs.VolumeKind, name string) string {
	id, _ := naming.EncodeVolID(pool, kind, sanitizeID(name))

	return id
}

func parseVolumeID(volumeID string) (naming.ParsedVolID, bool) {
	p, err := naming.ParseVolID(volumeID)

	return p, err == nil
}

func parseSnapshotID(snapshotID string) (naming.ParsedVolID, string, bool) {
	p, name, err := naming.ParseSnapID(snapshotID)

	return p, name, err == nil
}

// readinessPollContext reserves a small response window before a caller's RPC
// deadline. Without an RPC deadline, retain the bounded five-second fallback.
func readinessPollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		budget := time.Until(deadline) - pollDeadlineSafetyMargin
		if budget <= 0 {
			return context.WithTimeout(ctx, 0)
		}

		return context.WithTimeout(ctx, budget)
	}

	return context.WithTimeout(ctx, pollFallbackDeadline)
}

func waitStatusError(message string, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s: %v", message, err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s: %v", message, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", message, err)
	}
}

func (s *ControllerServer) waitForReady(ctx context.Context, crName string) error {
	pollCtx, cancel := readinessPollContext(ctx)
	defer cancel()

	return wait.PollUntilContextCancel(pollCtx, pollInterval, true, func(ctx context.Context) (bool, error) {
		vol := &zfscsiv1.Volume{}
		if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			return false, fmt.Errorf("get volume %s: %w", crName, err)
		}

		if vol.Status.CurrentState() == zfscsiv1.VolumeStateReady || vol.Status.CurrentState() == zfscsiv1.VolumeStateReadyToPublish {
			return true, nil
		}

		if vol.Status.CurrentState() == zfscsiv1.VolumeStateError {
			return false, fmt.Errorf("%w: %s", errVolumeInErrorState, crName)
		}

		return false, nil
	})
}

func (s *ControllerServer) waitForPublishedInitiator(ctx context.Context, crName, initiatorID string) error {
	pollCtx, cancel := readinessPollContext(ctx)
	defer cancel()

	return wait.PollUntilContextCancel(pollCtx, pollInterval, true, func(ctx context.Context) (bool, error) {
		vol := &zfscsiv1.Volume{}
		if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, vol); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			return false, fmt.Errorf("get volume %s/%s: %w", s.namespace, crName, err)
		}

		// Agent owns publishedInitiators — membership proves the agent actually
		// applied the configfs allow-host mapping.
		return slices.Contains(vol.Status.PublishedInitiators, initiatorID), nil
	})
}

// patchMappedInitiatorWithRetry writes only status.mappedInitiators with an
// optimistic-lock precondition; on 409 conflict it re-gets and retries.
func (s *ControllerServer) patchMappedInitiatorWithRetry(ctx context.Context, vol *zfscsiv1.Volume, mapped zfscsiv1.MappedInitiator, cap *csi.VolumeCapability) error {
	const maxRetries = 5

	for range maxRetries {
		// Capture baseline BEFORE mutation so the patch diff is non-empty.
		patch := crclient.MergeFromWithOptions(vol.DeepCopy(), crclient.MergeFromWithOptimisticLock{})

		if isSingleNodeWriterPublish(cap) {
			vol.Status.MappedInitiators = []zfscsiv1.MappedInitiator{mapped}
		} else {
			vol.Status.MappedInitiators = upsertInitiator(vol.Status.MappedInitiators, mapped)
		}

		if err := s.client.Status().Patch(ctx, vol, patch); err != nil {
			if apierrors.IsConflict(err) {
				// Re-read and retry.
				if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: vol.Name}, vol); err != nil {
					return fmt.Errorf("re-get on conflict: %w", err)
				}

				continue
			}

			return err
		}

		return nil
	}

	return errors.New("patch mappedInitiators: exceeded retry limit")
}

func (s *ControllerServer) waitForSnapshotReady(ctx context.Context, crName string) error {
	pollCtx, cancel := readinessPollContext(ctx)
	defer cancel()

	return wait.PollUntilContextCancel(pollCtx, pollInterval, true, func(ctx context.Context) (bool, error) {
		snap := &zfscsiv1.Snapshot{}
		if err := s.client.Get(ctx, apimachinerytypes.NamespacedName{Name: crName}, snap); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			return false, fmt.Errorf("get snapshot %s: %w", crName, err)
		}

		if snap.Status.Ready() {
			return true, nil
		}

		if snap.Status.CurrentState() == zfscsiv1.SnapshotStateError {
			return false, fmt.Errorf("%w: %s", errSnapshotInErrorState, crName)
		}

		return false, nil
	})
}

func (s *ControllerServer) topology(vol *zfscsiv1.Volume) []*csi.Topology {
	if vol != nil && vol.Spec.NetworkDomain != "" {
		return []*csi.Topology{{Segments: map[string]string{reachability.TopologyKeyNetworkDomain: vol.Spec.NetworkDomain}}}
	}
	// Return persisted network-domain topology when set; nil is only the fallback
	// when spec.networkDomain is empty.
	// Note the same-pool clone invariant (docs/storage-model.md): a volume created
	// from a snapshot/PVC source is pinned to the source's pool by `zfs clone`, but
	// that constraint is enforced at CreateVolume (validateVolumeContentSource
	// rejects cross-pool sources) rather than via topology, because a network-
	// reachable volume has no per-node placement requirement once it exists.
	return nil
}

func volBytes(req *csi.CreateVolumeRequest) int64 {
	if r := req.GetCapacityRange(); r != nil {
		return r.GetRequiredBytes()
	}

	return 0
}

// alignedCapacity applies the driver's block-volume capacity policy while
// preserving the CSI CapacityRange contract:
//
//   - required_bytes > limit_bytes is an incoherent range and is rejected.
//   - Filesystem (dataset) capacity is byte-exact: refquota has no alignment
//     constraint.
//   - Block (zvol) capacity is rounded UP to the next multiple of the effective
//     volblocksize, because ZFS rejects a volsize that is not a whole number of
//     volblocksize units.
//   - If limit_bytes is set and no aligned capacity fits at or below it, the
//     request is rejected rather than silently over-provisioning past the limit.
//
// blockSizeParam is the effective volblocksize for the volume: the StorageClass
// parameter for a fresh volume, or the source volume's persisted value for a
// clone/restore, which inherits volblocksize from its origin.
func alignedCapacity(required, limit int64, kind zfscsiv1.VolumeType, blockSizeParam string) (int64, error) {
	if required <= 0 {
		return 0, status.Error(codes.InvalidArgument, "capacity range required")
	}
	if limit > 0 && required > limit {
		return 0, status.Errorf(codes.InvalidArgument,
			"capacity range invalid: required_bytes %d exceeds limit_bytes %d", required, limit)
	}
	if kind != zfscsiv1.VolumeTypeBlock {
		return required, nil
	}

	blockSize, err := zfs.EffectiveBlockSize(blockSizeParam)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "block size: %v", err)
	}
	aligned, err := zfs.AlignUp(required, blockSize)
	if err != nil {
		return 0, status.Errorf(codes.OutOfRange, "align capacity to volblocksize: %v", err)
	}
	if limit > 0 && aligned > limit {
		return 0, status.Errorf(codes.InvalidArgument,
			"no capacity aligned to volblocksize %d bytes fits the requested range [%d, %d]: smallest aligned capacity is %d",
			blockSize, required, limit, aligned)
	}

	return aligned, nil
}

func volumeKindFromParams(sp scParams) string { return sp.Type }

func validateCapabilities(caps []*csi.VolumeCapability, kind string) error {
	for _, c := range caps {
		if c == nil {
			return errNilCapability
		}

		// A filesystem (NFS) volume cannot serve a raw block device: NFS is a
		// file protocol, so a Block access type is unsatisfiable. Reject it so
		// CreateVolume fails fast and the PVC stays Pending (the CSI contract
		// for an unsupported volumeMode), rather than binding a PV that can
		// never be published as a block device.
		if kind == string(zfscsiv1.VolumeTypeFilesystem) && c.GetBlock() != nil {
			return errBlockOnFilesystem
		}

		am := c.GetAccessMode().GetMode()
		// Filesystem volumes: allow multi-node (RWX over NFS).
		// Block volumes: only single-node (RWO).
		switch kind {
		case "filesystem":
			switch am {
			case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
			case csi.VolumeCapability_AccessMode_UNKNOWN,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
				return fmt.Errorf("%w: filesystem %s", errUnsupportedAccessMode, am)
			default:
				return fmt.Errorf("%w: filesystem %s", errUnsupportedAccessMode, am)
			}
		case "block":
			switch am {
			case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
				// MULTI_NODE_READER_ONLY (ROX): all consumers mount read-only, so
				// there is no writer and no cache-coherency hazard. The node marks
				// the block device read-only at the kernel layer (BLKROSET), and
				// nvmet maps every reader's initiator additively. Safe for shared
				// golden images (e.g. KubeVirt RO base disks).
				csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				// MULTI_NODE_MULTI_WRITER (RWX): the driver presents the SAME NVMe
				// namespace read/write to multiple initiators. The driver does NOT
				// provide cross-node write coordination — a raw block device is not
				// a cluster filesystem. The CONSUMER must coordinate writes (a
				// cluster FS like GFS2/OCFS2, or an app that owns the handoff, e.g.
				// KubeVirt/qemu during live migration). Uncoordinated concurrent
				// writes WILL corrupt. Advertised so those coordinated consumers can
				// use it; unsafe for a naive local filesystem.
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
			case csi.VolumeCapability_AccessMode_UNKNOWN,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
				// MULTI_NODE_SINGLE_WRITER (one RW node + RO on the rest) needs
				// asymmetric per-initiator RW/RO enforcement the transport does not
				// yet implement; reject rather than silently present all-RW.
				csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER:
				return fmt.Errorf("%w: block %s", errUnsupportedAccessMode, am)
			default:
				return fmt.Errorf("%w: block %s", errUnsupportedAccessMode, am)
			}
		}
	}

	return nil
}

func validateCapabilitiesMessage(caps []*csi.VolumeCapability, kind string) string {
	if err := validateCapabilities(caps, kind); err != nil {
		return err.Error()
	}

	return ""
}

func upsertInitiator(in []zfscsiv1.MappedInitiator, m zfscsiv1.MappedInitiator) []zfscsiv1.MappedInitiator {
	for i, e := range in {
		if e.NodeName == m.NodeName {
			in[i] = m

			return in
		}
	}

	return append(in, m)
}

func isSingleNodeWriterPublish(cap *csi.VolumeCapability) bool {
	if cap.GetAccessMode() == nil {
		return false
	}

	switch cap.GetAccessMode().GetMode() {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER:
		return true
	default:
		return false
	}
}

func removeInitiator(in []zfscsiv1.MappedInitiator, nodeName string) []zfscsiv1.MappedInitiator {
	out := in[:0]
	for _, e := range in {
		if e.NodeName != nodeName {
			out = append(out, e)
		}
	}

	return out
}

func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))

	return hex.EncodeToString(h[:])[:16]
}

func crNameFor(name string) string {
	// k8s names: lowercase, [a-z0-9-], max 253. Sanitise + prefix.
	id := sanitizeID(name)
	if len(id) > maxCRNameLen {
		id = id[:maxCRNameLen]
	}

	if strings.HasPrefix(id, "-") {
		id = "v" + id[1:]
	}

	return id
}

func sanitizeID(s string) string {
	// Delegate to naming.SanitizeLeaf — the canonical sanitizer handles length
	// (hash-truncation) so callers don't diverge.
	return naming.SanitizeLeaf(s)
}

func nqnFor(vol *zfscsiv1.Volume, p naming.ParsedVolID) string {
	n, _ := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)

	return n
}

func publishContextForVolume(vol *zfscsiv1.Volume) map[string]string {
	context := map[string]string{
		publishContextProvenance:    string(vol.Spec.Provenance),
		publishContextNetworkDomain: vol.Spec.NetworkDomain,
	}
	if vol.Spec.Type == zfscsiv1.VolumeTypeBlock {
		if vol.Status.TargetNQN == "" || vol.Status.DeviceGUID == "" || vol.Status.PortalHost == "" || vol.Status.PortalPort == 0 {
			return nil
		}
		portal, err := reachability.JoinPortal(vol.Status.PortalHost, vol.Status.PortalPort)
		if err != nil {
			return nil
		}
		context[publishContextTargetNQN] = vol.Status.TargetNQN
		context[publishContextPortal] = portal
		context[publishContextNamespaceID] = fmt.Sprintf("%d", defaultNamespaceID)
		context[publishContextDeviceGUID] = vol.Status.DeviceGUID
		if vol.Spec.NVMeTLSEnabled {
			if vol.Spec.NVMeTLSPSKSecretName == "" {
				return nil
			}
			if vol.Status.PortalPort != 4421 {
				return nil
			}
			context[publishContextTLS] = "true"
			context[publishContextPSKSecret] = vol.Spec.NVMeTLSPSKSecretName
		}
		return context
	}
	if vol.Status.ExportPath == "" || vol.Status.NFSServer == "" || vol.Status.NFSRootPath == "" {
		return nil
	}
	context[publishContextExportPath] = vol.Status.ExportPath
	context[publishContextNFSServer] = vol.Status.NFSServer
	context[publishContextNFSRootPath] = vol.Status.NFSRootPath
	// tls is NFS-only. NodeStage parses it strictly before adding xprtsec=mtls.
	context[publishContextTLS] = strconv.FormatBool(vol.Spec.NFSTLSEnabled)
	return context
}

func volumeContextForVolume(vol *zfscsiv1.Volume) map[string]string {
	if vol.Spec.Type != zfscsiv1.VolumeTypeFilesystem {
		return nil
	}
	context := map[string]string{publishContextProvenance: string(vol.Spec.Provenance)}
	if vol.Status.ExportPath != "" && vol.Status.NFSServer != "" && vol.Status.NFSRootPath != "" {
		context[publishContextExportPath] = vol.Status.ExportPath
		context[publishContextNFSServer] = vol.Status.NFSServer
		context[publishContextNFSRootPath] = vol.Status.NFSRootPath
	}
	return context
}

func timestampp(unix int64) *timestamppb.Timestamp {
	if unix <= 0 {
		return nil
	}

	return timestamppb.New(time.Unix(unix, 0))
}

func ensureFinalizer(finalizers *[]string, finalizer string) {
	for _, existing := range *finalizers {
		if existing == finalizer {
			return
		}
	}

	*finalizers = append(*finalizers, finalizer)
}

func validateVolumeContentSource(src *csi.VolumeContentSource, pool string, kind zfs.VolumeKind) (snapshotID, volumeID string, err error) {
	if src == nil {
		return "", "", nil
	}

	switch s := src.GetType().(type) {
	case *csi.VolumeContentSource_Snapshot:
		snapshotID = s.Snapshot.GetSnapshotId()
		if snapshotID == "" {
			return "", "", status.Error(codes.InvalidArgument, "snapshot content source id required")
		}

		p, _, parseErr := naming.ParseSnapID(snapshotID)
		if parseErr != nil {
			return "", "", status.Errorf(codes.InvalidArgument, "parse source snapshot id: %v", parseErr)
		}
		if p.Pool != pool || p.Kind != kind {
			return "", "", status.Errorf(codes.InvalidArgument, "source snapshot %s does not match requested pool/type %s/%s", snapshotID, pool, kind)
		}

		return snapshotID, "", nil

	case *csi.VolumeContentSource_Volume:
		volumeID = s.Volume.GetVolumeId()
		if volumeID == "" {
			return "", "", status.Error(codes.InvalidArgument, "volume content source id required")
		}

		p, parseErr := naming.ParseVolID(volumeID)
		if parseErr != nil {
			return "", "", status.Errorf(codes.InvalidArgument, "parse source volume id: %v", parseErr)
		}
		if p.Pool != pool || p.Kind != kind {
			return "", "", status.Errorf(codes.InvalidArgument, "source volume %s does not match requested pool/type %s/%s", volumeID, pool, kind)
		}

		return "", volumeID, nil
	default:
		return "", "", status.Error(codes.InvalidArgument, "unsupported volume content source")
	}
}

// volumeContentSource builds the CSI VolumeContentSource for restored/cloned volumes.
func volumeContentSource(req *csi.CreateVolumeRequest) *csi.VolumeContentSource {
	src := req.GetVolumeContentSource()
	if src == nil {
		return nil
	}

	return src
}
