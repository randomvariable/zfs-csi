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

package performance

import "testing"

func TestStorageClassForFilesystemRequiresNFSExportCIDRs(t *testing.T) {
	if _, err := StorageClassFor(Variant{StorageClass: "perf-nfs"}, "nfs", "tank", nil); err == nil {
		t.Fatal("accepted filesystem StorageClass without NFS export CIDRs")
	}
}

func TestStorageClassForFilesystemUsesExplicitNFSExportCIDRs(t *testing.T) {
	sc, err := StorageClassFor(Variant{StorageClass: "perf-nfs"}, "nfs", "tank", []string{"10.42.0.0/16", "2001:db8::/64"})
	if err != nil {
		t.Fatalf("StorageClassFor: %v", err)
	}
	if got := sc.Parameters["nfsExportCIDRs"]; got != "10.42.0.0/16,2001:db8::/64" {
		t.Fatalf("nfsExportCIDRs = %q", got)
	}
}
