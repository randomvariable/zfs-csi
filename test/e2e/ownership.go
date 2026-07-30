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

// e2eRunIDLabel is the ownership label applied to per-run resources.
//
// It lives in a non-test file (alongside e2eOwnershipLabels) because the
// ownership contract is referenced by the non-test e2e harness (driver_install,
// storage_helpers, pod_certificate_acceptance); symbols defined only in _test.go
// files are invisible to `go build -tags e2e ./...` (the CGO validation gate).
const e2eRunIDLabel = "zfs-csi.randomvariable.co.uk/e2e-run-id"

// e2eOwnershipLabels returns the ownership labels for e2e-managed resources.
func e2eOwnershipLabels(runID string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "zfs-csi-e2e",
		"app.kubernetes.io/managed-by": "ginkgo-e2e",
		e2eRunIDLabel:                  runID,
	}
}
