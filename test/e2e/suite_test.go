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

import (
	"fmt"
	"regexp"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

const (
	// cniPathVariable names the CNI manifest variable the framework applies
	// after control-plane init; its value lives in the per-lane e2e-config.yaml
	// (calico.yaml for KubeVirt, calico-aws-native.yaml for AWS).
	cniPathVariable = "CNI"

	// e2eNamespace is the single shared namespace for ALL e2e runs: golden
	// DataSource, per-run clusters, NADs. Co-locating the golden DataSource
	// eliminates cross-namespace CDI clone RBAC. Multiple clusters coexist,
	// differentiated by cluster name (derived from runID).
	e2eNamespace = "zfs-csi-e2e-images"
)

var e2eRunIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$`)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "zfs-csi CAPI/CAPK E2E Suite")
}

// requireE2EConfig validates the required E2E knobs and returns the run ID. It
// fails the suite early with an actionable message if RunID or Config are unset.
func requireE2EConfig() string {
	if err := e2econfig.Validate(); err != nil {
		Fail(err.Error())
	}
	runID := e2econfig.RunID()
	if !e2eRunIDPattern.MatchString(runID) {
		Fail(fmt.Sprintf("%s must match %s", e2econfig.Env[e2econfig.RunIDKey], e2eRunIDPattern.String()))
	}
	return runID
}

// perRunName derives a unique cluster name within the shared e2e namespace.
// The namespace is fixed (e2eNamespace); only the cluster name varies per run.
func perRunName(runID string) string {
	return "r" + runID
}
