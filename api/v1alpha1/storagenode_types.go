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

package v1alpha1

import (
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// StorageNodeConditionReady indicates whether inventory is eligible for use.
	StorageNodeConditionReady = "Ready"
	// StorageProtocolNFS identifies kernel NFS serving endpoints.
	StorageProtocolNFS StorageProtocol = "nfs"
	// StorageProtocolNVMeTCP identifies NVMe-over-TCP serving endpoints.
	StorageProtocolNVMeTCP StorageProtocol = "nvme-tcp"
)

// StorageProtocol identifies one network storage protocol.
type StorageProtocol string

// StorageNodeSpec contains operator-owned storage-node identity and policy.
type StorageNodeSpec struct {
	// authoritativePoolGUIDs is the immutable complete set of ZFS pool GUIDs owned by this logical storage node.
	// +required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=20
	// +kubebuilder:validation:items:Pattern=`^[1-9][0-9]{0,19}$`
	// +kubebuilder:validation:XValidation:rule="self.all(x, uint(x) > 0 && string(uint(x)) == x)",message="pool GUIDs must be canonical non-zero decimal uint64 values"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="authoritativePoolGUIDs is immutable"
	// +listType=set
	AuthoritativePoolGUIDs []string `json:"authoritativePoolGUIDs,omitempty"`

	// enabled controls whether this storage node is eligible.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// networkDomain identifies one worker/storage reachability domain.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self.matches('^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$')",message="networkDomain must be a valid Kubernetes label value"
	NetworkDomain string `json:"networkDomain"`
}

// ValidateAuthoritativePoolGUIDs verifies a complete canonical non-zero uint64 GUID set.
func ValidateAuthoritativePoolGUIDs(guids []string) error {
	if len(guids) == 0 {
		return fmt.Errorf("authoritativePoolGUIDs must contain at least one GUID")
	}
	seen := make(map[string]struct{}, len(guids))
	for _, guid := range guids {
		value, err := strconv.ParseUint(guid, 10, 64)
		if err != nil || value == 0 || strconv.FormatUint(value, 10) != guid {
			return fmt.Errorf("authoritativePoolGUIDs contains invalid canonical non-zero decimal uint64 %q", guid)
		}
		if _, exists := seen[guid]; exists {
			return fmt.Errorf("authoritativePoolGUIDs contains duplicate GUID %q", guid)
		}
		seen[guid] = struct{}{}
	}
	return nil
}

// StorageNodePoolStatus is one completely observed ZFS pool.
type StorageNodePoolStatus struct {
	// guid is the canonical nonzero decimal ZFS pool GUID.
	// +required
	// +kubebuilder:validation:MinLength=1
	GUID string `json:"guid"`

	// name is the raw node-local ZFS pool name.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// freeBytes is observed pool free space.
	// +required
	// +kubebuilder:validation:Minimum=0
	FreeBytes int64 `json:"freeBytes"`

	// capacityObservedAt identifies the pool-free sample used for placement accounting.
	// +required
	CapacityObservedAt metav1.Time `json:"capacityObservedAt"`

	// ready is true only for an ONLINE pool.
	// +required
	Ready bool `json:"ready"`
}

// StorageNodeEndpoint is one owner-local serving address. Host and port remain
// separate so IPv6 is rendered only with net.JoinHostPort at use boundaries.
type StorageNodeEndpoint struct {
	// protocol identifies the serving protocol.
	// +required
	// +kubebuilder:validation:Enum=nfs;nvme-tcp
	Protocol StorageProtocol `json:"protocol"`

	// host is a DNS name or unbracketed IPv4/IPv6 literal without a port.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Host string `json:"host"`

	// port is the TCP service port.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// StorageNodeStatus contains storage-agent-owned observations.
type StorageNodeStatus struct {
	// conditions carries inventory health.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// observedGeneration is spec generation observed by storage agent.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// lastObservedTime is timestamp of complete observation attempt.
	// +optional
	LastObservedTime *metav1.Time `json:"lastObservedTime,omitempty"`

	// reachableFrom is the complete set of consumer network domains able to
	// reach every published endpoint on this storage owner.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	ReachableFrom []string `json:"reachableFrom,omitempty"`

	// endpoints is the complete set of owner-local serving endpoints.
	// +optional
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=protocol
	// +listMapKey=host
	// +listMapKey=port
	Endpoints []StorageNodeEndpoint `json:"endpoints,omitempty"`

	// pools is complete replacement inventory keyed by stable pool GUID.
	// +optional
	// +listType=map
	// +listMapKey=guid
	Pools []StorageNodePoolStatus `json:"pools,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=zsn

// StorageNode advertises storage inventory for one Kubernetes Node.
type StorageNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageNodeSpec   `json:"spec"`
	Status StorageNodeStatus `json:"status,omitempty"`
}

// Enabled reports defaulted eligibility without requiring callers to dereference.
func (n *StorageNode) IsEnabled() bool {
	return n.Spec.Enabled == nil || *n.Spec.Enabled
}

// +kubebuilder:object:root=true

// StorageNodeList contains StorageNode resources.
type StorageNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageNode `json:"items"`
}
