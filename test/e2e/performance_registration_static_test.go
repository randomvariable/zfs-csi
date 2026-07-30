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

package e2e

import (
	"embed"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

//go:embed capi_lifecycle_test.go performance_test.go performance_disabled_test.go
var registrationSources embed.FS

func TestPerformanceRegistrationBuildPaths(t *testing.T) {
	lifecycle := readRegistrationSource(t, "capi_lifecycle_test.go")
	performance := readRegistrationSource(t, "performance_test.go")
	disabled := readRegistrationSource(t, "performance_disabled_test.go")

	registration := "RegisterPerformanceAcceptance(func() error {"
	if got := strings.Count(lifecycle, registration); got != 1 {
		t.Fatalf("lifecycle performance registrations = %d, want 1", got)
	}
	conformance := strings.Index(lifecycle, "It(\"runs the upstream external-storage conformance suite\"")
	registered := strings.Index(lifecycle, registration)
	if conformance == -1 || registered <= conformance {
		t.Fatal("performance registration must follow upstream conformance")
	}
	for _, want := range []string{
		"if cleanupOnly {\n\t\t\treturn nil\n\t\t}",
		"RunPerformanceAcceptance(ctx, artifactDir, workloadCluster, workloadClient, storage, driverImage)",
	} {
		if !strings.Contains(lifecycle, want) {
			t.Fatalf("lifecycle registration missing %q", want)
		}
	}

	if !strings.Contains(performance, "//go:build e2e && performance") {
		t.Fatal("performance registration must require e2e and performance tags")
	}
	if got := countCalls(t, "performance_test.go", "It"); got != 1 {
		t.Fatalf("performance build Ginkgo cases = %d, want 1", got)
	}

	if !strings.Contains(disabled, "//go:build e2e && !performance") {
		t.Fatal("ordinary e2e build must use the disabled registration counterpart")
	}
	if got := countCalls(t, "performance_disabled_test.go", "It"); got != 0 {
		t.Fatalf("ordinary e2e build Ginkgo cases = %d, want 0", got)
	}
}

func readRegistrationSource(t *testing.T, name string) string {
	t.Helper()
	source, err := registrationSources.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(source)
}

func countCalls(t *testing.T, name, function string) int {
	t.Helper()
	source, err := registrationSources.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		identifier, ok := call.Fun.(*ast.Ident)
		if ok && identifier.Name == function {
			count++
		}
		return true
	})
	return count
}
