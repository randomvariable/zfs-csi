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

package transport

import (
	"context"
	"sort"
	"sync"
)

var errInitiatorNotMapped = errSentinel("transport: initiator not mapped")

// Fake is a concurrency-safe in-memory transport for tests.
type Fake struct {
	mu      sync.Mutex
	targets map[string]*fakeTarget
}

type fakeTarget struct {
	ref         TargetRef
	initiators  map[string]bool
	connections map[string]bool
}

// New returns an empty fake transport implementing Server and Client.
func New() *Fake { return &Fake{targets: map[string]*fakeTarget{}} }

func (f *Fake) Export(ctx context.Context, opts ExportOptions) (TargetRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ref := TargetRef{Kind: opts.Kind, TargetNQN: opts.TargetNQN, Portal: opts.Portal, NamespaceID: 1, DeviceGUID: opts.DeviceGUID}
	if ref.Kind == "" {
		ref.Kind = KindNVMeTCP
	}

	if ref.TargetNQN == "" {
		ref.TargetNQN = opts.ZvolPath
	}

	key := fakeKey(ref)
	if _, ok := f.targets[key]; ok {
		return ref, ErrAlreadyExported
	}

	f.targets[key] = &fakeTarget{ref: ref, initiators: map[string]bool{}, connections: map[string]bool{}}

	return ref, nil
}

func (f *Fake) Unexport(ctx context.Context, ref TargetRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.targets, fakeKey(ref))

	return nil
}

func (f *Fake) MapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return ErrNotExported
	}

	t.initiators[initiatorID] = true

	return nil
}

func (f *Fake) UnmapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return nil
	}

	delete(t.initiators, initiatorID)
	delete(t.connections, initiatorID)

	return nil
}

func (f *Fake) MappedInitiators(ctx context.Context, ref TargetRef) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return nil, ErrNotExported
	}

	out := make([]string, 0, len(t.initiators))
	for id := range t.initiators {
		out = append(out, id)
	}

	sort.Strings(out)

	return out, nil
}

// ForceDisconnect models the kernel port-link bounce: every established
// controller of the subsystem is terminated (all connections cleared),
// regardless of allow-list membership. Legitimate initiators still present in
// t.initiators would reconnect in the real kernel; the fake leaves them mapped
// (allow-list intact) but disconnected, so a test asserting "the fenced host's
// connection is dead" sees connections cleared while the allow-list is
// unchanged. Idempotent; missing target -> success (already fenced).
func (f *Fake) ForceDisconnect(ctx context.Context, ref TargetRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return nil
	}

	for id := range t.connections {
		t.connections[id] = false
	}

	return nil
}

func (f *Fake) Attach(ctx context.Context, ref TargetRef, initiatorID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return "", ErrNotExported
	}

	if !t.initiators[initiatorID] {
		return "", errInitiatorNotMapped
	}

	t.connections[initiatorID] = true

	return "/dev/nvme" + itoa(ref.NamespaceID) + "n1", nil
}

func (f *Fake) Detach(ctx context.Context, ref TargetRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if t, ok := f.targets[fakeKey(ref)]; ok {
		for id := range t.connections {
			t.connections[id] = false
		}
	}

	return nil
}

// SetConnection lets tests model an externally-observed live connection.
func (f *Fake) SetConnection(ref TargetRef, initiatorID string, connected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if t, ok := f.targets[fakeKey(ref)]; ok {
		t.connections[initiatorID] = connected
	}
}

// activeConnection reports whether any mapped initiator currently has a live
// connection. Test-support accessor for the fake's modelled connection state
// (there is no production HasActiveConnection: the transport fences via
// ForceDisconnect rather than probing liveness).
func (f *Fake) activeConnection(ref TargetRef) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.targets[fakeKey(ref)]
	if !ok {
		return false
	}
	for id, connected := range t.connections {
		if connected && t.initiators[id] {
			return true
		}
	}

	return false
}

// Targets returns a stable snapshot keyed by target NQN.
func (f *Fake) Targets() map[string]TargetRef {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[string]TargetRef, len(f.targets))
	for k, t := range f.targets {
		out[k] = t.ref
	}

	return out
}

func fakeKey(ref TargetRef) string {
	return ref.TargetNQN
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte

	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
