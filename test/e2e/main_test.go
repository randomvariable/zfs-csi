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
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/spf13/pflag"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

// TestMain registers the E2E flags and initialises config once for the E2E
// suite. The default unit lane tests e2econfig as its own package; the
// `-tags=e2e` build includes this setup, E2E helpers, and the Ginkgo suite.
//
// Both need e2econfig.Init() so viper's AutomaticEnv resolves env vars such as
// E2E_DRIVER_IMAGE (used by driverImageFromEnv). We do NOT call pflag.Parse()
// here: the test binary also receives ginkgo flags (-ginkgo.v etc.) on the
// stdlib flag.CommandLine, and pflag.Parse() would choke on those. E2E values
// flow through env vars (mage sets them via ChildEnv), which AutomaticEnv
// resolves in Init() without pflag being parsed.
func TestMain(m *testing.M) {
	crlog.SetLogger(crzap.New(crzap.WriteTo(GinkgoWriter), crzap.UseDevMode(true)))
	e2econfig.Register(pflag.CommandLine)
	if err := e2econfig.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "init e2e config: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
