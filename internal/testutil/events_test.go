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

package testutil

import (
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRecorderCapturesEventMethods(t *testing.T) {
	regarding := &metav1.PartialObjectMetadata{}
	related := &metav1.PartialObjectMetadata{}
	recorder := &Recorder{}

	recorder.Eventf(regarding, related, "Normal", "Ready", "Provisioning", "ready %d", 1)

	events := recorder.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if got := events[0]; got.Type != "Normal" || got.Reason != "Ready" || got.Action != "Provisioning" || got.Note != "ready 1" || got.Related != related {
		t.Fatalf("Eventf record = %#v", got)
	}
}

func TestRecorderIsSafeForConcurrentEvents(t *testing.T) {
	recorder := &Recorder{}
	regarding := &metav1.PartialObjectMetadata{}
	const calls = 32

	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			recorder.Eventf(regarding, nil, "Normal", "Ready", "Provisioning", "ready")
		})
	}
	wg.Wait()

	if got := len(recorder.Events()); got != calls {
		t.Fatalf("events = %d, want %d", got, calls)
	}
}
