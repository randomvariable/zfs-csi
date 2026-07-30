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

package stage

import (
	"context"
	"errors"
	"sync"

	"github.com/randomvariable/zfs-csi/internal/mount"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

// errInjected is an arbitrary error injected into a recording fake to assert
// error-mapping in the server.
var errInjected = errors.New("injected test error")

// recordingMount is a fake mount.MountOps that records calls. Not a kernel
// simulation — it records method invocations + configured returns so tests can
// assert ordering + idempotent skip.
type recordingMount struct {
	mu sync.Mutex

	formatted map[string]bool // keyed by device+fsType

	formatCalls  []string // device
	mountCalls   []mountCall
	deviceBinds  []mountCall     // raw-block device binds (BindMountDevice)
	unmountCalls []string        // target
	resizeCalls  []mountCall     // filesystem resizes (device+target)
	mountPresent map[string]bool // keyed by target

	// injectable errors
	formatErr  error
	mountErr   error
	unmountErr error
}

type mountCall struct {
	source, target, fsType string
	opts                   []string
}

func newRecordingMount() *recordingMount {
	return &recordingMount{
		formatted:    map[string]bool{},
		mountPresent: map[string]bool{},
	}
}

func (r *recordingMount) Format(_ context.Context, device, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.formatCalls = append(r.formatCalls, device)

	if r.formatErr != nil {
		return r.formatErr
	}

	r.formatted[device] = true

	return nil
}

func (r *recordingMount) IsFormatted(_ context.Context, device, _ string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.formatted[device], nil
}

func (r *recordingMount) Mount(_ context.Context, source, target, fsType string, opts []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mountCalls = append(r.mountCalls, mountCall{source: source, target: target, fsType: fsType, opts: opts})

	if r.mountErr != nil {
		return r.mountErr
	}

	r.mountPresent[target] = true

	return nil
}

func (r *recordingMount) Unmount(_ context.Context, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.unmountCalls = append(r.unmountCalls, target)

	if r.unmountErr != nil {
		return r.unmountErr
	}

	r.mountPresent[target] = false

	return nil
}

func (r *recordingMount) IsMounted(_ context.Context, _, target string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.mountPresent[target], nil
}

func (r *recordingMount) BindMount(_ context.Context, _, _ string, _ bool) error {
	return errors.New("bind not supported by recordingMount")
}

func (r *recordingMount) DeviceFromMount(_ context.Context, _ string) (string, error) {
	return "", errors.New("device from mount not supported by recordingMount")
}

func (r *recordingMount) BindMountDevice(_ context.Context, device, target string, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mountErr != nil {
		return r.mountErr
	}
	r.deviceBinds = append(r.deviceBinds, mountCall{source: device, target: target})
	if r.mountPresent == nil {
		r.mountPresent = map[string]bool{}
	}
	r.mountPresent[target] = true

	return nil
}

func (r *recordingMount) Resize(_ context.Context, device, target, fsType string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resizeCalls = append(r.resizeCalls, mountCall{source: device, target: target, fsType: fsType})

	return nil
}

// transport.Fake already implements transport.Client — these compile-time
// assertions guard the contract stays met.
var (
	_ transport.Client          = (*transport.Fake)(nil)
	_ mount.MountOps            = (*recordingMount)(nil)
	_ stagepb.StagePluginServer = (*NVMeStagePlugin)(nil)
	_ stagepb.StagePluginServer = (*NFSStagePlugin)(nil)
)
