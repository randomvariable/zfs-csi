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

// Package testutil contains reusable test fixtures.
package testutil

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

// EventRecord is one Kubernetes Event captured by Recorder.
type EventRecord struct {
	Regarding runtime.Object
	Related   runtime.Object
	Type      string
	Reason    string
	Action    string
	Note      string
}

// Recorder captures Kubernetes Events for assertions. It is safe for concurrent
// reconcilers and implements the events.k8s.io/v1 recorder interface.
type Recorder struct {
	mu     sync.Mutex
	record []EventRecord
}

// Eventf records an events.k8s.io/v1 Event call.
func (r *Recorder) Eventf(regarding, related runtime.Object, eventType, reason, action, note string, args ...any) {
	r.add(EventRecord{
		Regarding: regarding,
		Related:   related,
		Type:      eventType,
		Reason:    reason,
		Action:    action,
		Note:      fmt.Sprintf(note, args...),
	})
}

// Events returns a snapshot of recorded Events.
func (r *Recorder) Events() []EventRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]EventRecord(nil), r.record...)
}

func (r *Recorder) add(event EventRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.record = append(r.record, event)
}

var _ events.EventRecorder = (*Recorder)(nil)
