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
	"encoding/json"
	"fmt"
)

type FioResult struct {
	Jobs []FioJob `json:"jobs"`
}
type FioJob struct {
	Read  FioDirection `json:"read"`
	Write FioDirection `json:"write"`
}
type FioDirection struct {
	IOPS    float64 `json:"iops"`
	BWBytes float64 `json:"bw_bytes"`
	CLatNS  struct {
		Percentile map[string]float64 `json:"percentile"`
	} `json:"clat_ns"`
}

func ParseFio(raw []byte, operation string) (iops, throughputMiB, p99Millis float64, err error) {
	var result FioResult
	if err = json.Unmarshal(raw, &result); err != nil {
		return 0, 0, 0, err
	}
	if len(result.Jobs) == 0 {
		return 0, 0, 0, fmt.Errorf("fio result has no jobs")
	}
	for _, job := range result.Jobs {
		d := job.Read
		if operation == "write" {
			d = job.Write
		}
		iops += d.IOPS
		throughputMiB += d.BWBytes / (1024 * 1024)
		if p := d.CLatNS.Percentile["99.000000"] / 1e6; p > p99Millis {
			p99Millis = p
		}
	}
	return iops, throughputMiB, p99Millis, nil
}

type FioWorkload struct {
	Name, RW, BlockSize              string
	QueueDepth, Jobs, RuntimeSeconds int
}

var Workloads = []FioWorkload{
	{Name: "4k-randread", RW: "randread", BlockSize: "4k", QueueDepth: 32, Jobs: 4, RuntimeSeconds: 60},
	{Name: "4k-randwrite", RW: "randwrite", BlockSize: "4k", QueueDepth: 32, Jobs: 4, RuntimeSeconds: 60},
	{Name: "1m-read", RW: "read", BlockSize: "1m", QueueDepth: 32, Jobs: 1, RuntimeSeconds: 60},
	{Name: "1m-write", RW: "write", BlockSize: "1m", QueueDepth: 32, Jobs: 1, RuntimeSeconds: 60},
}
