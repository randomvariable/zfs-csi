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

package driver

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// F9: blockPathAbnormal flags a non-live NVMe controller state as abnormal, and
// treats live / absent state as normal.
func TestBlockPathAbnormal_NVMeControllerState(t *testing.T) {
	tests := []struct {
		name         string
		state        string // "" means do not create the state file
		wantAbnormal bool
	}{
		{name: "live is normal", state: "live", wantAbnormal: false},
		{name: "connecting is abnormal", state: "connecting", wantAbnormal: true},
		{name: "deleting is abnormal", state: "deleting", wantAbnormal: true},
		{name: "resetting is abnormal", state: "resetting", wantAbnormal: true},
		{name: "absent state file is normal (best-effort)", state: "", wantAbnormal: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sysBlock := t.TempDir()
			// Simulate /dev node name nvme1n1.
			devDir := filepath.Join(sysBlock, "nvme1n1", "device")
			if err := os.MkdirAll(devDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if tt.state != "" {
				if err := os.WriteFile(filepath.Join(devDir, "state"), []byte(tt.state+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			abnormal, msg := blockPathAbnormal(sysBlock, "/dev/nvme1n1")
			if abnormal != tt.wantAbnormal {
				t.Fatalf("abnormal = %v (%q), want %v", abnormal, msg, tt.wantAbnormal)
			}
		})
	}
}

// F9: a hung statfs (slow volumeUsage) reports a timeout within the deadline
// rather than wedging, and repeated calls do NOT leak a goroutine per call
// (dedup guard suppresses new spawns while one is stuck).
func TestVolumeUsageBounded_TimeoutAndDedup(t *testing.T) {
	// Point the probe at a path whose statfs blocks. We cannot make a real
	// statfs hang portably, so instead drive the dedup+timeout logic directly by
	// shrinking the timeout and pre-marking the path as in-flight.
	orig := statfsProbeTimeout
	statfsProbeTimeout = 20 * time.Millisecond
	defer func() { statfsProbeTimeout = orig }()

	n := &NodeServer{}
	const path = "/mnt/hung"

	// Pre-mark the path in-flight to simulate a previously-stuck probe.
	if !n.beginProbe(path) {
		t.Fatal("first beginProbe should succeed")
	}

	before := runtime.NumGoroutine()

	// Many calls while a probe is "stuck": each must return errStatfsTimeout
	// immediately and spawn NO new goroutine (dedup guard).
	for range 50 {
		_, err := n.volumeUsageBounded(context.Background(), path)
		if err != errStatfsTimeout {
			t.Fatalf("err = %v, want errStatfsTimeout while probe outstanding", err)
		}
	}

	// Allow the scheduler to settle; assert no goroutine growth from the loop.
	time.Sleep(10 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+2 { // small slack for test-runtime noise
		t.Fatalf("goroutine growth = %d (before=%d after=%d); dedup guard leaked goroutines", after-before, before, after)
	}

	// Release the guard; a subsequent call may now spawn a real probe.
	n.endProbe(path)
	if !n.beginProbe(path) {
		t.Fatal("beginProbe should succeed after endProbe")
	}
	n.endProbe(path)
}
