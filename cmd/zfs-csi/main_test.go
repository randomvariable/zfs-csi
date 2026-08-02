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

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/randomvariable/zfs-csi/internal/agent"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type nfsdTestLogSink struct {
	records []string
}

func (s *nfsdTestLogSink) Init(logr.RuntimeInfo) {}
func (s *nfsdTestLogSink) Enabled(int) bool      { return true }
func (s *nfsdTestLogSink) Info(_ int, msg string, keysAndValues ...any) {
	s.records = append(s.records, msg+" "+fmt.Sprint(keysAndValues...))
}
func (s *nfsdTestLogSink) Error(err error, msg string, keysAndValues ...any) {
	s.records = append(s.records, msg+" "+err.Error()+" "+fmt.Sprint(keysAndValues...))
}
func (s *nfsdTestLogSink) WithValues(...any) logr.LogSink { return s }
func (s *nfsdTestLogSink) WithName(string) logr.LogSink   { return s }

func TestNFSRuntimeCompositionSharesControllerAndStopsAfterWorkers(t *testing.T) {
	runtime := newNFSRuntimeComponents(logr.Discard())
	reconciler := &agent.VolumeReconciler{}
	runtime.wireVolumeReconciler(reconciler)
	if reconciler.NFSRootController != runtime.rootController || reconciler.NFSExports != runtime.exports || reconciler.NFSFlusher != runtime.responder {
		t.Fatal("volume reconciler did not receive shared NFS runtime")
	}

	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	var mu sync.Mutex
	activeWorkers := 0
	stopWrites := 0
	startWorker := func(ctx context.Context) {
		mu.Lock()
		activeWorkers++
		mu.Unlock()
		<-ctx.Done()
		mu.Lock()
		activeWorkers--
		mu.Unlock()
	}
	runtime.runResponder = func(ctx context.Context) error { startWorker(ctx); return nil }
	runtime.runController = startWorker
	nfsdProcFS = nfsdProcFSOps{WriteFile: func(_ string, data []byte, _ os.FileMode) error {
		mu.Lock()
		defer mu.Unlock()
		if activeWorkers != 0 {
			t.Errorf("nfsd stopped with %d active workers", activeWorkers)
		}
		if string(data) == "0\n" {
			stopWrites++
		}
		return nil
	}}

	var runnables []manager.Runnable
	if err := runtime.add(func(r manager.Runnable) error {
		runnables = append(runnables, r)
		return nil
	}, &nfsdLifecycle{owned: true}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(runnables) != 3 {
		t.Fatalf("runnables = %d, want 3", len(runnables))
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, len(runnables))
	for _, runnable := range runnables {
		go func(r manager.Runnable) { done <- r.Start(ctx) }(runnable)
	}
	deadline := time.After(time.Second)
	for {
		mu.Lock()
		started := activeWorkers == 2
		mu.Unlock()
		if started {
			break
		}
		select {
		case <-deadline:
			t.Fatal("NFS workers did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	for range runnables {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if activeWorkers != 0 {
		t.Fatalf("active workers = %d, want 0", activeWorkers)
	}
	if stopWrites != 1 {
		t.Fatalf("nfsd stop writes = %d, want 1", stopWrites)
	}
}

func TestStartNFSDLifecycleOrdersKernelConfigurationAndOwnsThreads(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })

	var calls []string
	files := map[string][]byte{
		"/proc/fs/nfsd/nfsv4gracetime": []byte("0\n"),
		"/proc/fs/nfsd/nfsv4leasetime": []byte("0\n"),
		"/proc/fs/nfsd/threads":        []byte("0\n"),
		"/proc/fs/nfsd/portlist":       []byte("\n"),
	}
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(path string) ([]byte, error) {
			calls = append(calls, "read "+path)
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return data, nil
		},
		WriteFile: func(path string, data []byte, _ os.FileMode) error {
			calls = append(calls, "write "+path+"="+string(data))
			switch path {
			case "/proc/fs/nfsd/portlist":
				files[path] = []byte("tcp 2049\n")
			case "/proc/fs/nfsd/threads":
				files[path] = data
			default:
				files[path] = data
			}
			return nil
		},
		Mount: func(source, target, fstype string, flags uintptr, data string) error {
			calls = append(calls, "mount "+source+" "+target+" "+fstype)
			files["/proc/fs/nfsd/versions"] = []byte("-2 -3 +4 +4.1 +4.2\n")
			return nil
		},
		MkdirAll: func(path string, mode os.FileMode) error { return nil },
	}

	lifecycle, err := startNFSDLifecycle(logr.Discard())
	if err != nil {
		t.Fatalf("startNFSDLifecycle() error = %v", err)
	}
	if !lifecycle.owned {
		t.Fatal("lifecycle did not acquire ownership")
	}
	if got, want := string(files["/proc/fs/nfsd/threads"]), "8\n"; got != want {
		t.Fatalf("threads = %q, want %q", got, want)
	}
	got, want := calls, []string{
		"read /proc/fs/nfsd/versions",
		"mount nfsd /proc/fs/nfsd nfsd",
		"read /proc/fs/nfsd/threads",
		"write /proc/fs/nfsd/versions=-2 -3 +4 +4.1 +4.2\n",
		"read /proc/fs/nfsd/nfsv4gracetime",
		"write /proc/fs/nfsd/nfsv4gracetime=90\n",
		"read /proc/fs/nfsd/nfsv4leasetime",
		"write /proc/fs/nfsd/nfsv4leasetime=90\n",
		"write /proc/fs/nfsd/portlist=tcp 2049\n",
		"read /proc/fs/nfsd/portlist",
		"write /proc/fs/nfsd/threads=8\n",
		"read /proc/fs/nfsd/threads",
	}
	want = append(want[:3], append([]string{"read /sys/kernel/debug/nfsd/delegated_timestamps", "mount debugfs /sys/kernel/debug debugfs", "read /sys/kernel/debug/nfsd/delegated_timestamps"}, want[3:]...)...)
	if len(got) != len(want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("calls[%d] = %q, want %q; calls = %#v", i, got[i], want[i], calls)
		}
	}
}

func TestConfigureNFSDDelegatedTimestamps(t *testing.T) {
	tests := []struct {
		name       string
		initial    string
		writeErr   error
		readback   string
		wantErr    string
		wantWrites int
	}{
		{name: "enabled", initial: "Y\n", readback: "N\n", wantWrites: 1},
		{name: "disabled", initial: "N\n", wantWrites: 0},
		{name: "unsupported", wantWrites: 0},
		{name: "malformed", initial: "1\n", wantErr: "want Y or N", wantWrites: 0},
		{name: "write failure", initial: "Y\n", writeErr: syscall.EACCES, wantErr: "disable delegated timestamps", wantWrites: 1},
		{name: "readback still enabled", initial: "Y\n", readback: "Y\n", wantErr: "remains enabled", wantWrites: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := nfsdProcFS
			t.Cleanup(func() { nfsdProcFS = old })
			path := "/sys/kernel/debug/nfsd/delegated_timestamps"
			writes := 0
			nfsdProcFS = nfsdProcFSOps{
				ReadFile: func(name string) ([]byte, error) {
					if name != path || tc.name == "unsupported" {
						return nil, os.ErrNotExist
					}
					if writes == 0 {
						return []byte(tc.initial), nil
					}
					return []byte(tc.readback), nil
				},
				WriteFile: func(name string, _ []byte, _ os.FileMode) error {
					if name != path {
						t.Fatalf("write path = %q, want %q", name, path)
					}
					writes++
					return tc.writeErr
				},
				Mount:    func(string, string, string, uintptr, string) error { return nil },
				MkdirAll: func(string, os.FileMode) error { return nil },
			}

			err := configureNFSDDelegatedTimestamps(logr.Discard())
			if tc.wantErr == "" && err != nil {
				t.Fatalf("configureNFSDDelegatedTimestamps() error = %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("configureNFSDDelegatedTimestamps() error = %v, want %q", err, tc.wantErr)
			}
			if writes != tc.wantWrites {
				t.Fatalf("writes = %d, want %d", writes, tc.wantWrites)
			}
		})
	}
}

func TestConfigureNFSDDelegatedTimestampsFailsClosedWhenDebugFSMountFails(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	mountErr := errors.New("debugfs mount denied")
	writes := 0
	nfsdProcFS = nfsdProcFSOps{
		ReadFile:  func(string) ([]byte, error) { return nil, os.ErrNotExist },
		WriteFile: func(string, []byte, os.FileMode) error { writes++; return nil },
		Mount:     func(string, string, string, uintptr, string) error { return mountErr },
		MkdirAll:  func(string, os.FileMode) error { return nil },
	}
	if err := configureNFSDDelegatedTimestamps(logr.Discard()); err == nil || !errors.Is(err, mountErr) {
		t.Fatalf("configureNFSDDelegatedTimestamps() error = %v, want debugfs mount error", err)
	}
	if writes != 0 {
		t.Fatalf("writes = %d, want 0 after failed debugfs mount", writes)
	}
}

func TestStartNFSDLifecycleSkipsUnsupportedOptionalControls(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })

	var calls []string
	files := map[string][]byte{
		"/proc/fs/nfsd/threads":  []byte("0\n"),
		"/proc/fs/nfsd/portlist": []byte("\n"),
	}
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(path string) ([]byte, error) {
			calls = append(calls, "read "+path)
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return data, nil
		},
		WriteFile: func(path string, data []byte, _ os.FileMode) error {
			calls = append(calls, "write "+path+"="+string(data))
			if path == "/proc/fs/nfsd/portlist" {
				files[path] = []byte("tcp 2049\n")
			} else if path == "/proc/fs/nfsd/threads" {
				files[path] = data
			}
			return nil
		},
		Mount: func(source, target, fstype string, flags uintptr, data string) error {
			calls = append(calls, "mount "+source+" "+target+" "+fstype)
			files["/proc/fs/nfsd/versions"] = []byte("-2 -3 +4 +4.1 +4.2\n")
			return nil
		},
		MkdirAll: func(path string, mode os.FileMode) error { return nil },
	}

	if _, err := startNFSDLifecycle(logr.Discard()); err != nil {
		t.Fatalf("startNFSDLifecycle() error = %v", err)
	}
	for _, call := range calls {
		if strings.Contains(call, "nfsv4gracetime") || strings.Contains(call, "nfsv4leasetime") || strings.Contains(call, "/grace") || strings.Contains(call, "/lease") {
			if strings.HasPrefix(call, "write ") {
				t.Fatalf("unsupported optional control was written: %q", call)
			}
		}
	}
}

func TestStartNFSDLifecycleDoesNotUseLegacyOptionalControls(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	files := map[string][]byte{
		"/proc/fs/nfsd/grace": []byte("0\n"), "/proc/fs/nfsd/lease": []byte("0\n"),
		"/proc/fs/nfsd/threads": []byte("0\n"), "/proc/fs/nfsd/portlist": []byte("\n"),
	}
	var writes []string
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(path string) ([]byte, error) {
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return data, nil
		},
		WriteFile: func(path string, data []byte, _ os.FileMode) error {
			writes = append(writes, path+"="+string(data))
			if path == "/proc/fs/nfsd/portlist" {
				files[path] = []byte("tcp 2049\n")
			}
			if path == "/proc/fs/nfsd/threads" {
				files[path] = data
			}
			return nil
		},
		Mount: func(string, string, string, uintptr, string) error {
			files["/proc/fs/nfsd/versions"] = []byte("-2 -3 +4 +4.1 +4.2\n")
			return nil
		},
		MkdirAll: func(string, os.FileMode) error { return nil },
	}
	if _, err := startNFSDLifecycle(logr.Discard()); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(writes, ","), "/proc/fs/nfsd/versions=-2 -3 +4 +4.1 +4.2\n,/proc/fs/nfsd/portlist=tcp 2049\n,/proc/fs/nfsd/threads=8\n"; got != want {
		t.Fatalf("writes = %q, want %q", got, want)
	}
}

func TestStartNFSDLifecycleSkipsDefaultOptionalControls(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	files := map[string][]byte{
		"/proc/fs/nfsd/nfsv4gracetime": []byte("90\n"),
		"/proc/fs/nfsd/nfsv4leasetime": []byte("90\n"),
		"/proc/fs/nfsd/threads":        []byte("0\n"),
		"/proc/fs/nfsd/portlist":       []byte("\n"),
	}
	var writes []string
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(path string) ([]byte, error) {
			if path == filepath.Join(nfsdPath, "versions") {
				return nil, os.ErrNotExist
			}
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return data, nil
		},
		WriteFile: func(path string, data []byte, _ os.FileMode) error {
			writes = append(writes, path+"="+string(data))
			if path == filepath.Join(nfsdPath, "portlist") {
				files[path] = []byte("tcp 2049\n")
			}
			if path == filepath.Join(nfsdPath, "threads") {
				files[path] = data
			}
			return nil
		},
		Mount:    func(string, string, string, uintptr, string) error { return nil },
		MkdirAll: func(string, os.FileMode) error { return nil },
	}
	if _, err := startNFSDLifecycle(logr.Discard()); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(writes, ","), "/proc/fs/nfsd/versions=-2 -3 +4 +4.1 +4.2\n,/proc/fs/nfsd/portlist=tcp 2049\n,/proc/fs/nfsd/threads=8\n"; got != want {
		t.Fatalf("writes = %q, want %q", got, want)
	}
}

func TestStartNFSDLifecycleIgnoresOptionalControlPermissionErrors(t *testing.T) {
	for _, writeErr := range []error{syscall.EACCES, syscall.EBUSY} {
		t.Run(writeErr.Error(), func(t *testing.T) {
			old := nfsdProcFS
			t.Cleanup(func() { nfsdProcFS = old })
			files := map[string][]byte{
				filepath.Join(nfsdPath, "nfsv4gracetime"): []byte("0\n"),
				filepath.Join(nfsdPath, "nfsv4leasetime"): []byte("0\n"),
				filepath.Join(nfsdPath, "threads"):        []byte("0\n"),
				filepath.Join(nfsdPath, "portlist"):       []byte("\n"),
			}
			var threadWrites int
			sink := &nfsdTestLogSink{}
			nfsdProcFS = nfsdProcFSOps{
				ReadFile: func(path string) ([]byte, error) {
					if path == filepath.Join(nfsdPath, "versions") {
						return nil, os.ErrNotExist
					}
					if data, ok := files[path]; ok {
						return data, nil
					}
					return nil, os.ErrNotExist
				},
				WriteFile: func(path string, data []byte, _ os.FileMode) error {
					switch path {
					case filepath.Join(nfsdPath, "nfsv4gracetime"), filepath.Join(nfsdPath, "nfsv4leasetime"):
						return writeErr
					case filepath.Join(nfsdPath, "portlist"):
						files[path] = []byte("tcp 2049\n")
					case filepath.Join(nfsdPath, "threads"):
						threadWrites++
						files[path] = data
					}
					return nil
				},
				Mount:    func(string, string, string, uintptr, string) error { return nil },
				MkdirAll: func(string, os.FileMode) error { return nil },
			}
			if _, err := startNFSDLifecycle(logr.New(sink)); err != nil {
				t.Fatalf("startNFSDLifecycle() error = %v", err)
			}
			if len(sink.records) < 2 || !strings.Contains(sink.records[1], "controlnfsv4gracetime") || !strings.Contains(sink.records[1], "desired90") {
				t.Fatalf("logs = %#v, want structured control and desired fields", sink.records)
			}
			if threadWrites != 1 {
				t.Fatalf("thread writes = %d, want 1", threadWrites)
			}
		})
	}
}

func TestStartNFSDLifecycleFailsOnMalformedOptionalControl(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(path string) ([]byte, error) {
			if strings.HasSuffix(path, "versions") {
				return nil, os.ErrNotExist
			}
			if strings.HasSuffix(path, "nfsv4gracetime") {
				return []byte("not-a-number\n"), nil
			}
			if strings.HasSuffix(path, "threads") {
				return []byte("0\n"), nil
			}
			return []byte("\n"), nil
		},
		WriteFile: func(string, []byte, os.FileMode) error { return nil },
		Mount:     func(string, string, string, uintptr, string) error { return nil },
		MkdirAll:  func(string, os.FileMode) error { return nil },
	}
	if _, err := startNFSDLifecycle(logr.Discard()); err == nil || !strings.Contains(err.Error(), "parse current value") {
		t.Fatalf("startNFSDLifecycle() error = %v, want malformed value error", err)
	}
}

func TestStartNFSDLifecycleFailsOnOptionalControlProbeError(t *testing.T) {
	for _, name := range []string{"nfsv4gracetime", "nfsv4leasetime"} {
		t.Run(name, func(t *testing.T) {
			old := nfsdProcFS
			t.Cleanup(func() { nfsdProcFS = old })
			probeErr := errors.New("probe failed")
			writes := 0
			nfsdProcFS = nfsdProcFSOps{
				ReadFile: func(path string) ([]byte, error) {
					if path == delegatedTimestampsPath {
						return nil, os.ErrNotExist
					}
					if strings.HasSuffix(path, "versions") {
						return nil, os.ErrNotExist
					}
					if strings.HasSuffix(path, "threads") {
						return []byte("0\n"), nil
					}
					if strings.HasSuffix(path, name) {
						return nil, probeErr
					}
					return nil, os.ErrNotExist
				},
				WriteFile: func(string, []byte, os.FileMode) error { writes++; return nil },
				Mount:     func(string, string, string, uintptr, string) error { return nil },
				MkdirAll:  func(string, os.FileMode) error { return nil },
			}
			if _, err := startNFSDLifecycle(logr.Discard()); err == nil || !errors.Is(err, probeErr) {
				t.Fatalf("startNFSDLifecycle() error = %v, want probe error", err)
			}
			if writes != 1 {
				t.Fatalf("writes = %d, want 1", writes)
			}
		})
	}
}

func TestNFSDLifecycleFailurePaths(t *testing.T) {
	tests := []struct {
		name       string
		threads    string
		portlist   string
		started    string
		wantWrites int
	}{
		{name: "collision", threads: "2\n", portlist: "", wantWrites: 0},
		{name: "missing listener", threads: "0\n", portlist: "udp 2049\ntcp 2050\ntcp 2048\n", wantWrites: 2},
		{name: "zero started threads", threads: "0\n", portlist: "udp 2049\ntcp 2049\n", started: "0\n", wantWrites: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := nfsdProcFS
			t.Cleanup(func() { nfsdProcFS = old })
			writes := 0
			reads := 0
			nfsdProcFS = nfsdProcFSOps{
				ReadFile: func(path string) ([]byte, error) {
					if path == delegatedTimestampsPath {
						return nil, os.ErrNotExist
					}
					if strings.HasSuffix(path, "versions") {
						return nil, os.ErrNotExist
					}
					if strings.HasSuffix(path, "threads") {
						reads++
						if reads > 1 && tc.started != "" {
							return []byte(tc.started), nil
						}
						return []byte(tc.threads), nil
					}
					if strings.HasSuffix(path, "nfsv4gracetime") || strings.HasSuffix(path, "nfsv4leasetime") {
						return nil, os.ErrNotExist
					}
					return []byte(tc.portlist), nil
				},
				WriteFile: func(string, []byte, os.FileMode) error { writes++; return nil },
				Mount:     func(string, string, string, uintptr, string) error { return nil },
				MkdirAll:  func(string, os.FileMode) error { return nil },
			}
			if _, err := startNFSDLifecycle(logr.Discard()); err == nil {
				t.Fatal("startNFSDLifecycle() succeeded")
			}
			if writes != tc.wantWrites {
				t.Fatalf("writes = %d, want %d", writes, tc.wantWrites)
			}
		})
	}
}

func TestNFSDLifecycleStopOwnership(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	writes := 0
	nfsdProcFS = nfsdProcFSOps{WriteFile: func(_ string, data []byte, _ os.FileMode) error {
		if string(data) == "0\n" {
			writes++
		}
		return nil
	}}
	lifecycle := &nfsdLifecycle{owned: true}
	if err := lifecycle.stop(); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.stop(); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("stop writes = %d, want 1", writes)
	}
	if err := (&nfsdLifecycle{}).stop(); err != nil {
		t.Fatal(err)
	}
	if err := (*nfsdLifecycle)(nil).stop(); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("unowned stop writes = %d, want 1", writes)
	}
}

func TestNFSDLifecycleStopCachesFailure(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	stopErr := errors.New("stop failed")
	writes := 0
	nfsdProcFS = nfsdProcFSOps{WriteFile: func(string, []byte, os.FileMode) error {
		writes++
		return stopErr
	}}
	lifecycle := &nfsdLifecycle{owned: true}
	if err := lifecycle.stop(); !errors.Is(err, stopErr) {
		t.Fatalf("first stop error = %v, want %v", err, stopErr)
	}
	if err := lifecycle.stop(); !errors.Is(err, stopErr) {
		t.Fatalf("second stop error = %v, want cached %v", err, stopErr)
	}
	if writes != 1 {
		t.Fatalf("stop writes = %d, want 1", writes)
	}
}

func TestMountNFSDProcFSDoesNotStackOnProbeFailure(t *testing.T) {
	old := nfsdProcFS
	t.Cleanup(func() { nfsdProcFS = old })
	probeErr := errors.New("permission denied")
	mounts := 0
	nfsdProcFS = nfsdProcFSOps{
		ReadFile: func(string) ([]byte, error) { return nil, probeErr },
		MkdirAll: func(string, os.FileMode) error { t.Fatal("mkdir called after non-ENOENT probe failure"); return nil },
		Mount:    func(string, string, string, uintptr, string) error { mounts++; return nil },
	}
	if err := mountNFSDProcFS(); err == nil || !errors.Is(err, probeErr) {
		t.Fatalf("mountNFSDProcFS() error = %v, want wrapped probe error", err)
	}
	if mounts != 0 {
		t.Fatalf("mounts = %d, want 0", mounts)
	}
}

func TestPortalAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		host string
		want string
	}{
		{name: "empty", want: ""},
		{name: "bare host", host: "storage-0", want: "storage-0:4420"},
		{name: "ipv4", host: "10.0.0.7", want: "10.0.0.7:4420"},
		{name: "explicit port", host: "storage-0:4421", want: "storage-0:4421"},
		{name: "ipv6", host: "2001:db8::7", want: "[2001:db8::7]:4420"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := portalAddress(tc.host); got != tc.want {
				t.Fatalf("portalAddress(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

func TestValidateExpectedOwner(t *testing.T) {
	for _, test := range []struct {
		name      string
		nodeName  string
		expected  string
		wantError string
	}{
		{name: "legacy unchecked", nodeName: "storage-a"},
		{name: "matching owner", nodeName: "storage-a", expected: "storage-a"},
		{name: "missing node name", expected: "storage-a", wantError: "NODE_NAME"},
		{name: "mismatch", nodeName: "storage-b", expected: "storage-a", wantError: "does not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateExpectedOwner(test.nodeName, test.expected)
			if test.wantError == "" && err != nil {
				t.Fatalf("validateExpectedOwner = %v, want nil", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("validateExpectedOwner = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestGetInClusterNodePerformsExactlyOneNamedGet(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes/worker-a" || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s?%s, want one named Node GET", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"worker-a"}}`))
	}))
	t.Cleanup(server.Close)

	node, err := getInClusterNode(t.Context(), "worker-a",
		func() (*rest.Config, error) { return &rest.Config{Host: server.URL}, nil },
		kubernetes.NewForConfig,
	)
	if err != nil || node.Name != "worker-a" {
		t.Fatalf("getInClusterNode = %#v, %v", node, err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want exactly 1", requests)
	}
}

func TestGetInClusterNodeFailsBeforeRequestOnConfigError(t *testing.T) {
	_, err := getInClusterNode(context.Background(), "worker-a",
		func() (*rest.Config, error) { return nil, context.Canceled },
		kubernetes.NewForConfig,
	)
	if err == nil || !strings.Contains(err.Error(), "in-cluster") {
		t.Fatalf("getInClusterNode error = %v", err)
	}
}
