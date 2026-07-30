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

import (
	"math"
	"strings"
	"testing"
)

func TestDefaultScenariosValidateAndDoNotReuseStorageClasses(t *testing.T) {
	if err := ValidateScenarios(DefaultScenarios()); err != nil {
		t.Fatal(err)
	}
}

func TestNFSScenariosUnavailableWithOneConsumer(t *testing.T) {
	scenarios := DefaultScenarios()
	for i := range scenarios {
		if scenarios[i].Transport == "nfs" {
			scenarios[i].Available, scenarios[i].SkipReason = false, "NFS comparison requires two ready non-storage workers"
		}
	}
	runnable, nfsUnavailable := 0, 0
	for _, scenario := range scenarios {
		if scenario.Available {
			runnable++
		}
		if scenario.Transport == "nfs" && !scenario.Available {
			nfsUnavailable++
		}
	}
	if runnable == 0 || nfsUnavailable == 0 {
		t.Fatalf("runnable=%d nfsUnavailable=%d", runnable, nfsUnavailable)
	}
}

func TestValidateScenariosRejectsClassReuse(t *testing.T) {
	s := DefaultScenarios()
	s[1].Control.StorageClass = s[0].Control.StorageClass
	if err := ValidateScenarios(s); err == nil {
		t.Fatal("accepted reused StorageClass")
	}
}

func TestUniqueStorageClassesAreImmutablePerRun(t *testing.T) {
	a := uniqueStorageClasses(DefaultScenarios(), "run-a")
	b := uniqueStorageClasses(DefaultScenarios(), "run-b")
	if a[0].Control.StorageClass == b[0].Control.StorageClass {
		t.Fatal("class name was reused across runs")
	}
}

func TestFioManifestUsesWorkloadValuesAndAffinity(t *testing.T) {
	w := FioWorkload{Name: "custom", RW: "randread", BlockSize: "8k", QueueDepth: 7, Jobs: 2, RuntimeSeconds: 11}
	job := FioJobManifest("ns", "job", "pvc", "fio@sha256:x", "worker-a", w, "measured")
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	for _, want := range []string{"--iodepth=7", "--numjobs=2", "--runtime=11"} {
		if !strings.Contains(args, want) {
			t.Fatalf("missing %s in %s", want, args)
		}
	}
	if job.Spec.Template.Spec.NodeName != "" || job.Spec.Template.Spec.Affinity == nil {
		t.Fatal("job must use required node affinity, not nodeName")
	}
}

func TestAssessRejectsNonFiniteAndNonPositive(t *testing.T) {
	valid := []float64{1, 1, 1}
	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if got := Assess("fio", "throughput", []float64{bad, 1, 1}, valid, false); got.Valid {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestAssessMetricDirections(t *testing.T) {
	for _, metric := range []string{"throughput", "iops"} {
		if got := Assess("fio", metric, []float64{100, 100, 100}, []float64{90, 90, 90}, false); got.Direction != "higher" || got.Passed {
			t.Fatalf("bad higher assessment: %+v", got)
		}
	}
	if got := Assess("fio", "p99-latency", []float64{10, 10, 10}, []float64{12, 12, 12}, false); got.Direction != "lower" || got.Passed {
		t.Fatalf("bad latency assessment: %+v", got)
	}
}

func TestAssessSamplesUsesDeclaredControlCandidateOrder(t *testing.T) {
	scenario := Scenario{Name: "invert", Control: Variant{Name: "z-control"}, Candidate: Variant{Name: "a-candidate"}}
	var samples []Sample
	for i := range 3 {
		samples = append(samples,
			Sample{Scenario: "invert", Variant: "z-control", Phase: "1m-read", Iteration: i, Valid: true, ThroughputMiB: 100, IOPS: 100, P99Millis: 10},
			Sample{Scenario: "invert", Variant: "a-candidate", Phase: "1m-read", Iteration: i, Valid: true, ThroughputMiB: 80, IOPS: 80, P99Millis: 12},
		)
	}
	got := AssessSamples([]Scenario{scenario}, samples)
	if len(got) != 3 {
		t.Fatalf("got %d assessments", len(got))
	}
	for _, assessment := range got {
		if assessment.Metric != "p99-latency" && assessment.PairedRelativeMedian >= 0 {
			t.Fatalf("control/candidate inverted: %+v", assessment)
		}
	}
}

func TestAssessInversionTrapsForBlocksizeAndFilesystem(t *testing.T) {
	for _, scenario := range []string{"nvme-sequential-blocksize-16k-vs-128k", "nvme-filesystem-xfs-vs-ext4"} {
		got := Assess(scenario, "throughput", []float64{100, 100, 100}, []float64{80, 80, 80}, false)
		if got.PairedRelativeMedian >= 0 || got.Passed {
			t.Fatalf("accepted inverted %s result: %+v", scenario, got)
		}
	}
}

func TestParseDiagnosticFactsFailsClosed(t *testing.T) {
	if _, err := ParseDiagnosticFacts("CPU_MODEL=x\nNIC=eth0\nMTU=nope\nSPEED=1000"); err == nil {
		t.Fatal("accepted malformed facts")
	}
}
