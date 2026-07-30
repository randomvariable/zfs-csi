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

import "fmt"

func DefaultScenarios() []Scenario {
	return []Scenario{
		{
			Name:      "nfs-nconnect",
			Transport: "nfs",
			Available: true,
			Control: Variant{
				Name:         "nconnect-1",
				StorageClass: "perf-nfs-nconnect-1",
				MountOptions: []string{"vers=4.2", "nconnect=1"},
			},
			Candidate: Variant{
				Name:         "nconnect-8",
				StorageClass: "perf-nfs-nconnect-8",
				MountOptions: []string{"vers=4.2", "nconnect=8"},
			},
		},
		{
			Name:      "nfs-version",
			Transport: "nfs",
			Available: true,
			Control: Variant{
				Name:         "nfs-4.1",
				StorageClass: "perf-nfs-41",
				MountOptions: []string{"vers=4.1", "nconnect=8"},
			},
			Candidate: Variant{
				Name:         "nfs-4.2",
				StorageClass: "perf-nfs-42",
				MountOptions: []string{"vers=4.2", "nconnect=8"},
			},
		},
		{
			Name:       "nfs-dataset-properties",
			Transport:  "nfs",
			Available:  false,
			SkipReason: "atime and xattr are creation defaults but are not exposed as StorageClass tunables",
		},
		{
			Name:      "nvme-random-blocksize",
			Transport: "nvme",
			Available: true,
			Control: Variant{
				Name:         "16k",
				StorageClass: "perf-nvme-random-16k",
				Parameters:   map[string]string{"blocksize": "16k"},
			},
			Candidate: Variant{
				Name:         "8k",
				StorageClass: "perf-nvme-random-8k",
				Parameters:   map[string]string{"blocksize": "8k"},
			},
		},
		{
			Name:      "nvme-sequential-blocksize",
			Transport: "nvme",
			Available: true,
			Control: Variant{
				Name:         "16k",
				StorageClass: "perf-nvme-sequential-16k",
				Parameters:   map[string]string{"blocksize": "16k"},
			},
			Candidate: Variant{
				Name:         "128k",
				StorageClass: "perf-nvme-sequential-128k",
				Parameters:   map[string]string{"blocksize": "128k"},
			},
		},
		{
			Name:      "nvme-filesystem",
			Transport: "nvme",
			Available: true,
			Control: Variant{
				Name:         "xfs",
				StorageClass: "perf-nvme-xfs",
				Parameters:   map[string]string{"csi.storage.k8s.io/fstype": "xfs"},
			},
			Candidate: Variant{
				Name:         "ext4",
				StorageClass: "perf-nvme-ext4",
				Parameters:   map[string]string{"csi.storage.k8s.io/fstype": "ext4"},
			},
		},
		{
			Name:       "finite-ctrl-loss-tmo",
			Transport:  "nvme",
			Available:  false,
			SkipReason: "finite ctrl_loss_tmo violates the retry-forever recovery invariant",
		},
		{
			Name:       "nvme-queue-tunables",
			Transport:  "nvme",
			Available:  false,
			SkipReason: "queue_size and nr_io_queues are not implemented",
		},
	}
}

func ValidateScenarios(scenarios []Scenario) error {
	names := map[string]struct{}{}
	classes := map[string]string{}
	for _, s := range scenarios {
		if s.Name == "" {
			return fmt.Errorf("scenario name is required")
		}
		if _, ok := names[s.Name]; ok {
			return fmt.Errorf("duplicate scenario %q", s.Name)
		}
		names[s.Name] = struct{}{}
		if s.Transport != "nfs" && s.Transport != "nvme" {
			return fmt.Errorf("scenario %s has invalid transport %q", s.Name, s.Transport)
		}
		if !s.Available {
			if s.SkipReason == "" {
				return fmt.Errorf("unavailable scenario %s requires skipReason", s.Name)
			}
			continue
		}
		for _, v := range []Variant{s.Control, s.Candidate} {
			if v.Name == "" || v.StorageClass == "" {
				return fmt.Errorf("scenario %s has incomplete variant", s.Name)
			}
			if owner, ok := classes[v.StorageClass]; ok && owner != s.Name {
				return fmt.Errorf("StorageClass %q reused by %s and %s", v.StorageClass, owner, s.Name)
			}
			classes[v.StorageClass] = s.Name
		}
		if s.Control.Name == s.Candidate.Name {
			return fmt.Errorf("scenario %s control and candidate names match", s.Name)
		}
	}
	return nil
}
