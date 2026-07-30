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

import "time"

const (
	WarmupCycles       = 5
	MeasuredCycles     = 20
	MinimumLifeSamples = 18
	FioMeasuredRuns    = 3
)

type Environment struct {
	Fingerprint string            `json:"fingerprint"`
	GitCommit   string            `json:"gitCommit"`
	DriverImage string            `json:"driverImage"`
	FioImage    string            `json:"fioImage"`
	Kubernetes  string            `json:"kubernetes"`
	Nodes       []NodeEnvironment `json:"nodes"`
	Values      map[string]string `json:"values,omitempty"`
	Pool        PoolEnvironment   `json:"pool"`
}

type NodeEnvironment struct {
	Name             string            `json:"name"`
	Kernel           string            `json:"kernel,omitempty"`
	Architecture     string            `json:"architecture"`
	ContainerRuntime string            `json:"containerRuntime"`
	Labels           map[string]string `json:"labels,omitempty"`
	CPUModel         string            `json:"cpuModel"`
	CPUs             int               `json:"cpus"`
	NICs             []NICEnvironment  `json:"nics"`
}

type NICEnvironment struct {
	Name      string `json:"name"`
	MTU       int    `json:"mtu"`
	SpeedMbps int    `json:"speedMbps"`
}

type PoolEnvironment struct {
	Name          string `json:"name"`
	Health        string `json:"health"`
	ZFSVersion    string `json:"zfsVersion"`
	Topology      string `json:"topology"`
	FreeBytes     uint64 `json:"freeBytes"`
	SizeBytes     uint64 `json:"sizeBytes"`
	Fragmentation int    `json:"fragmentation"`
}

type Run struct {
	SchemaVersion int          `json:"schemaVersion"`
	RunID         string       `json:"runID"`
	StartedAt     time.Time    `json:"startedAt"`
	Environment   Environment  `json:"environment"`
	Scenarios     []Scenario   `json:"scenarios"`
	Assessments   []Assessment `json:"assessments,omitempty"`
	Valid         bool         `json:"valid"`
	InvalidReason string       `json:"invalidReason,omitempty"`
}

type Scenario struct {
	Name       string            `json:"name"`
	Transport  string            `json:"transport"`
	Control    Variant           `json:"control"`
	Candidate  Variant           `json:"candidate"`
	Available  bool              `json:"available"`
	SkipReason string            `json:"skipReason,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

type Variant struct {
	Name         string            `json:"name"`
	StorageClass string            `json:"storageClass"`
	Parameters   map[string]string `json:"parameters,omitempty"`
	MountOptions []string          `json:"mountOptions,omitempty"`
}

type Sample struct {
	Scenario       string    `json:"scenario"`
	Variant        string    `json:"variant"`
	Phase          string    `json:"phase"`
	Iteration      int       `json:"iteration"`
	Warmup         bool      `json:"warmup"`
	StartedAt      time.Time `json:"startedAt"`
	DurationMillis float64   `json:"durationMillis,omitempty"`
	IOPS           float64   `json:"iops,omitempty"`
	ThroughputMiB  float64   `json:"throughputMiB,omitempty"`
	P99Millis      float64   `json:"p99Millis,omitempty"`
	Valid          bool      `json:"valid"`
	InvalidReason  string    `json:"invalidReason,omitempty"`
	RawArtifact    string    `json:"rawArtifact,omitempty"`
	Fingerprint    string    `json:"fingerprint"`
}

type Event struct {
	Time      time.Time         `json:"time"`
	Scenario  string            `json:"scenario"`
	Variant   string            `json:"variant"`
	Iteration int               `json:"iteration"`
	Boundary  string            `json:"boundary"`
	Object    string            `json:"object,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

type Summary struct {
	Count  int     `json:"count"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
	MAD    float64 `json:"mad"`
	CV     float64 `json:"cv"`
}

type Assessment struct {
	Scenario             string  `json:"scenario"`
	Metric               string  `json:"metric"`
	Control              Summary `json:"control"`
	Candidate            Summary `json:"candidate"`
	PairedRelativeMedian float64 `json:"pairedRelativeMedian"`
	ConfidenceLow        float64 `json:"confidenceLow"`
	ConfidenceHigh       float64 `json:"confidenceHigh"`
	Valid                bool    `json:"valid"`
	InvalidReason        string  `json:"invalidReason,omitempty"`
	Direction            string  `json:"direction"`
	Passed               bool    `json:"passed"`
}
