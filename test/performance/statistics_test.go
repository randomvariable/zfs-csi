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
	"math/rand"
	"reflect"
	"testing"
)

func TestSummarize(t *testing.T) {
	s := Summarize([]float64{1, 2, 3, 4, 5})
	if s.Median != 3 || s.P95 != 4.8 || s.MAD != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
}

func TestSummaryOrderInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	values := make([]float64, 200)
	for i := range values {
		values[i] = rng.Float64() * 1000
	}
	want := Summarize(values)
	for range 20 {
		rng.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
		if got := Summarize(values); !reflect.DeepEqual(got, want) {
			t.Fatalf("summary depends on input order: %+v != %+v", got, want)
		}
	}
}

func TestAssessDeterministic(t *testing.T) {
	control, candidate := make([]float64, 20), make([]float64, 20)
	for i := range control {
		control[i], candidate[i] = 100+float64(i%3), 90+float64(i%3)
	}
	a := Assess("attach", "duration", control, candidate, true)
	b := Assess("attach", "duration", control, candidate, true)
	if !a.Valid {
		t.Fatalf("assessment invalid: %s", a.InvalidReason)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("bootstrap output is not deterministic: %+v != %+v", a, b)
	}
	if a.PairedRelativeMedian >= 0 {
		t.Fatalf("expected improvement, got %f", a.PairedRelativeMedian)
	}
}

func TestAssessRejectsSampleCountAndVariance(t *testing.T) {
	if got := Assess("fio", "throughput", []float64{1, 1}, []float64{1, 1}, false); got.Valid {
		t.Fatal("accepted fewer than three fio samples")
	}
	control := make([]float64, 20)
	candidate := make([]float64, 20)
	for i := range control {
		control[i] = 100
		candidate[i] = 100
		if i%2 == 0 {
			candidate[i] = 200
		}
	}
	if got := Assess("attach", "duration", control, candidate, true); got.Valid {
		t.Fatal("accepted lifecycle CV over 20%")
	}
}

func TestEnvironmentStable(t *testing.T) {
	samples := []Sample{{Variant: "a", Iteration: 1, Fingerprint: "one"}, {Variant: "b", Iteration: 1, Fingerprint: "two"}}
	if EnvironmentStable("one", samples) == nil {
		t.Fatal("accepted environment drift")
	}
}

func TestAssessSamplesDropsWarmups(t *testing.T) {
	var samples []Sample
	for i := 0; i < 25; i++ {
		for _, variant := range []string{"control", "candidate"} {
			samples = append(samples, Sample{Scenario: "lifecycle", Variant: variant, Phase: "attach", Iteration: i, Warmup: i < 5, Valid: true, DurationMillis: 100})
		}
	}
	got := AssessSamples([]Scenario{{Name: "lifecycle", Control: Variant{Name: "control"}, Candidate: Variant{Name: "candidate"}}}, samples)
	if len(got) != 1 || !got[0].Valid || got[0].Control.Count != 20 {
		t.Fatalf("unexpected assessments: %+v", got)
	}
}
