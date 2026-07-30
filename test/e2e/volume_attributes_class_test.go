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

//go:build e2e

package e2e

import "testing"

func TestVACVolumeCRName(t *testing.T) {
	got, err := volumeCRName("csi:tank:block:vac-volume")
	if err != nil || got != "vac-volume" {
		t.Fatalf("volume CR name = %q, %v", got, err)
	}
	if _, err := volumeCRName("not-a-csi-volume"); err == nil {
		t.Fatal("malformed volume handle was accepted")
	}
}
