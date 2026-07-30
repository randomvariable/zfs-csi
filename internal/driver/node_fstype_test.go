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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestBlockFsType(t *testing.T) {
	t.Run("defaults to xfs", func(t *testing.T) {
		if got := blockFsType(nil); got != "xfs" {
			t.Fatalf("blockFsType(nil) = %q, want xfs", got)
		}
	})

	t.Run("honors capability override", func(t *testing.T) {
		cap := &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		}
		if got := blockFsType(cap); got != "ext4" {
			t.Fatalf("blockFsType(cap) = %q, want ext4", got)
		}
	})
}
