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

func TestLiveEnabledRequiresExactOptIn(t *testing.T) {
	t.Setenv("ZFS_CSI_PERF", "true")
	if LiveEnabled() {
		t.Fatal("accepted non-contract opt-in")
	}
	t.Setenv("ZFS_CSI_PERF", "1")
	if !LiveEnabled() {
		t.Fatal("rejected exact opt-in")
	}
}

func TestFioImageRequiresDigest(t *testing.T) {
	t.Setenv("ZFS_CSI_PERF_FIO_IMAGE", "example/fio:latest")
	if _, err := FioImage(); err == nil {
		t.Fatal("accepted mutable fio image")
	}
	t.Setenv("ZFS_CSI_PERF_FIO_IMAGE", "example/fio@sha256:abc")
	if _, err := FioImage(); err != nil {
		t.Fatal(err)
	}
}
