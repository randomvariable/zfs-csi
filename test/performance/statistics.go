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
	"fmt"
	"math"
	"math/rand"
	"sort"
)

const bootstrapSeed int64 = 690069

func Summarize(values []float64) Summary {
	if err := validateValues(values); err != nil {
		return Summary{CV: math.Inf(1)}
	}
	x := append([]float64(nil), values...)
	sort.Float64s(x)
	if len(x) == 0 {
		return Summary{}
	}
	mean := 0.0
	for _, v := range x {
		mean += v
	}
	mean /= float64(len(x))
	variance := 0.0
	for _, v := range x {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(len(x))
	med := percentile(x, 0.50)
	deviations := make([]float64, len(x))
	for i, v := range x {
		deviations[i] = math.Abs(v - med)
	}
	sort.Float64s(deviations)
	cv := 0.0
	if mean != 0 {
		cv = math.Sqrt(variance) / math.Abs(mean)
	}
	return Summary{Count: len(x), Median: med, P95: percentile(x, .95), P99: percentile(x, .99), MAD: percentile(deviations, .50), CV: cv}
}

func Assess(scenario, metric string, control, candidate []float64, lifecycle bool) Assessment {
	direction := metricDirection(metric)
	a := Assessment{Scenario: scenario, Metric: metric, Direction: direction}
	if err := validateValues(control); err != nil {
		a.InvalidReason = "control: " + err.Error()
		return a
	}
	if err := validateValues(candidate); err != nil {
		a.InvalidReason = "candidate: " + err.Error()
		return a
	}
	a.Control, a.Candidate = Summarize(control), Summarize(candidate)
	required, maxCV := FioMeasuredRuns, .10
	if lifecycle {
		required, maxCV = MinimumLifeSamples, .20
	}
	if len(control) < required || len(candidate) < required {
		a.InvalidReason = fmt.Sprintf("requires at least %d valid samples per variant", required)
		return a
	}
	if a.Control.CV > maxCV || a.Candidate.CV > maxCV {
		a.InvalidReason = fmt.Sprintf("coefficient of variation exceeds %.0f%%", maxCV*100)
		return a
	}
	n := len(control)
	if len(candidate) < n {
		n = len(candidate)
	}
	changes := make([]float64, n)
	for i := range n {
		if control[i] == 0 {
			a.InvalidReason = "control sample is zero"
			return a
		}
		changes[i] = (candidate[i] - control[i]) / control[i]
	}
	a.PairedRelativeMedian = medianSigned(changes)
	a.ConfidenceLow, a.ConfidenceHigh = bootstrapMedianCI(changes, 10000)
	a.Valid = true
	if direction == "lower" {
		a.Passed = a.ConfidenceHigh <= .10
	} else {
		a.Passed = a.ConfidenceLow > -.10
	}
	return a
}

func validateValues(values []float64) error {
	if len(values) == 0 {
		return fmt.Errorf("no samples")
	}
	for i, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
			return fmt.Errorf("sample %d is not finite and positive", i)
		}
	}
	return nil
}

func metricDirection(metric string) string {
	if metric == "attach" || metric == "detach" || metric == "p99-latency" {
		return "lower"
	}
	return "higher"
}

func EnvironmentStable(expected string, samples []Sample) error {
	for _, sample := range samples {
		if sample.Fingerprint != expected {
			return fmt.Errorf("environment drift: sample %s/%d fingerprint %q, expected %q", sample.Variant, sample.Iteration, sample.Fingerprint, expected)
		}
	}
	return nil
}

func AssessSamples(scenarios []Scenario, samples []Sample) []Assessment {
	type key struct{ scenario, phase string }
	grouped := map[key]map[string]map[string][]float64{}
	for _, sample := range samples {
		if !sample.Valid || sample.Warmup {
			continue
		}
		k := key{sample.Scenario, sample.Phase}
		if grouped[k] == nil {
			grouped[k] = map[string]map[string][]float64{}
		}
		if grouped[k][sample.Variant] == nil {
			grouped[k][sample.Variant] = map[string][]float64{}
		}
		if sample.DurationMillis > 0 {
			grouped[k][sample.Variant]["duration"] = append(grouped[k][sample.Variant]["duration"], sample.DurationMillis)
		} else {
			grouped[k][sample.Variant]["throughput"] = append(grouped[k][sample.Variant]["throughput"], sample.ThroughputMiB)
			grouped[k][sample.Variant]["iops"] = append(grouped[k][sample.Variant]["iops"], sample.IOPS)
			grouped[k][sample.Variant]["p99-latency"] = append(grouped[k][sample.Variant]["p99-latency"], sample.P99Millis)
		}
	}
	keys := make([]key, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scenario == keys[j].scenario {
			return keys[i].phase < keys[j].phase
		}
		return keys[i].scenario < keys[j].scenario
	})
	var assessments []Assessment
	for _, k := range keys {
		var scenario *Scenario
		for i := range scenarios {
			if scenarios[i].Name == k.scenario {
				scenario = &scenarios[i]
				break
			}
		}
		if scenario == nil || len(grouped[k]) != 2 {
			assessments = append(assessments, Assessment{Scenario: k.scenario, Metric: k.phase, InvalidReason: "requires exactly two variants"})
			continue
		}
		lifecycle := k.phase == "attach" || k.phase == "detach"
		metrics := []string{"throughput", "iops", "p99-latency"}
		if lifecycle {
			metrics = []string{"duration"}
		}
		for _, metric := range metrics {
			name := metric
			if lifecycle {
				name = k.phase
			}
			assessments = append(assessments, Assess(k.scenario, name, grouped[k][scenario.Control.Name][metric], grouped[k][scenario.Candidate.Name][metric], lifecycle))
		}
	}
	return assessments
}

func bootstrapMedianCI(values []float64, rounds int) (float64, float64) {
	rng := rand.New(rand.NewSource(bootstrapSeed)) // deterministic acceptance output
	medians := make([]float64, rounds)
	resample := make([]float64, len(values))
	for i := range rounds {
		for j := range resample {
			resample[j] = values[rng.Intn(len(values))]
		}
		medians[i] = medianSigned(resample)
	}
	sort.Float64s(medians)
	return percentile(medians, .025), percentile(medians, .975)
}

func medianSigned(values []float64) float64 {
	x := append([]float64(nil), values...)
	sort.Float64s(x)
	return percentile(x, .50)
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := p * float64(len(sorted)-1)
	lo, hi := int(math.Floor(pos)), int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(pos-float64(lo))
}
