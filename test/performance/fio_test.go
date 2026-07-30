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

func TestParseFio(t *testing.T) {
	raw := []byte(
		`{"jobs":[{"read":{"iops":1000,"bw_bytes":10485760,"clat_ns":{"percentile":{"99.000000":2500000}}},"write":{}},{"read":{"iops":500,"bw_bytes":5242880,"clat_ns":{"percentile":{"99.000000":3000000}}},"write":{}}]}`,
	)
	iops, bw, p99, err := ParseFio(raw, "read")
	if err != nil {
		t.Fatal(err)
	}
	if iops != 1500 || bw != 15 || p99 != 3 {
		t.Fatalf("unexpected fio parse: iops=%f bw=%f p99=%f", iops, bw, p99)
	}
}

func TestParseFioRejectsEmptyJobs(t *testing.T) {
	if _, _, _, err := ParseFio([]byte(`{"jobs":[]}`), "read"); err == nil {
		t.Fatal("accepted empty fio result")
	}
}
