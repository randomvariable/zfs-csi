// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRootRuntimeProbeChecksOnlyPreAuthStructure(t *testing.T) {
	old := statRootRuntimePath
	oldRead := readRootRuntimePath
	t.Cleanup(func() { statRootRuntimePath, readRootRuntimePath = old, oldRead })
	var paths []string
	statRootRuntimePath = func(path string) (os.FileInfo, error) {
		paths = append(paths, path)
		return fakeRootFileInfo{dir: path == "/tank"}, nil
	}
	readRootRuntimePath = healthyRootRuntimeRead

	if err := probeNFSDRuntime(t.Context(), "/tank"); err != nil {
		t.Fatal(err)
	}
	want := []string{"/tank", nfsdThreadsPath, nfsdPortlistPath}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths = %v, want %v", paths, want)
		}
	}
}

func TestRootRuntimeProbeNeverTouchesFilehandleResolution(t *testing.T) {
	old := statRootRuntimePath
	oldRead := readRootRuntimePath
	t.Cleanup(func() { statRootRuntimePath, readRootRuntimePath = old, oldRead })
	statRootRuntimePath = func(path string) (os.FileInfo, error) {
		if path == "/proc/fs/nfsd/filehandle" {
			t.Fatal("pre-client runtime probe touched filehandle resolution")
		}
		return fakeRootFileInfo{dir: path == "/tank"}, nil
	}
	readRootRuntimePath = healthyRootRuntimeRead

	if err := probeNFSDRuntime(t.Context(), "/tank"); err != nil {
		t.Fatal(err)
	}
}

func TestRootRuntimeProbeClassifiesAvailabilityAndPrivileges(t *testing.T) {
	old := statRootRuntimePath
	oldRead := readRootRuntimePath
	t.Cleanup(func() { statRootRuntimePath, readRootRuntimePath = old, oldRead })
	readRootRuntimePath = healthyRootRuntimeRead

	statRootRuntimePath = func(string) (os.FileInfo, error) { return nil, unix.ENOENT }
	if err := probeNFSDRuntime(context.Background(), "/tank"); !isRootPreflightRetryable(err) {
		t.Fatalf("ENOENT = %v, want retryable", err)
	}
	statRootRuntimePath = func(string) (os.FileInfo, error) { return nil, unix.EPERM }
	if err := probeNFSDRuntime(context.Background(), "/tank"); !errors.Is(err, errRootPreflightTerminalDeploy) {
		t.Fatalf("EPERM = %v, want terminal deployment", err)
	}
	statRootRuntimePath = func(string) (os.FileInfo, error) { return fakeRootFileInfo{}, nil }
	if err := probeNFSDRuntime(context.Background(), "/tank"); !isRootPreflightTerminalConfig(err) {
		t.Fatalf("file root = %v, want terminal config", err)
	}
}

func TestRootRuntimeProbeHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := probeNFSDRuntime(ctx, "/tank"); !isRootPreflightRetryable(err) {
		t.Fatalf("cancelled = %v, want retryable", err)
	}
}

func TestRootRuntimeProbeRequiresActiveNFSD(t *testing.T) {
	oldStat, oldRead := statRootRuntimePath, readRootRuntimePath
	t.Cleanup(func() { statRootRuntimePath, readRootRuntimePath = oldStat, oldRead })
	statRootRuntimePath = func(path string) (os.FileInfo, error) {
		return fakeRootFileInfo{dir: path == "/tank"}, nil
	}

	t.Run("threads", func(t *testing.T) {
		readRootRuntimePath = func(path string) ([]byte, error) {
			if path == nfsdThreadsPath {
				return []byte("0\n"), nil
			}
			return []byte("tcp 2049\n"), nil
		}
		if err := probeNFSDRuntime(t.Context(), "/tank"); !isRootPreflightRetryable(err) {
			t.Fatalf("zero threads = %v, want retryable", err)
		}
	})
	t.Run("listener", func(t *testing.T) {
		readRootRuntimePath = func(path string) ([]byte, error) {
			if path == nfsdThreadsPath {
				return []byte("8\n"), nil
			}
			return []byte("udp 111\n"), nil
		}
		if err := probeNFSDRuntime(t.Context(), "/tank"); !isRootPreflightRetryable(err) {
			t.Fatalf("missing 2049 = %v, want retryable", err)
		}
	})
}

func TestNFSDPortlistHasTCP2049ParsesExactRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{name: "exact", body: "tcp 2049\n", want: true},
		{name: "whitespace", body: "  tcp\t2049  \n", want: true},
		{name: "repeated", body: "udp 2049\ntcp 2049\ntcp 2049\n", want: true},
		{name: "wrong protocol", body: "udp 2049\n"},
		{name: "prefix service", body: "tcp 12049\n"},
		{name: "suffix service", body: "tcp 20490\n"},
		{name: "extra field", body: "tcp 2049 extra\n"},
		{name: "missing field", body: "tcp\n"},
		{name: "joined", body: "tcp2049\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nfsdPortlistHasTCP2049([]byte(tc.body)); got != tc.want {
				t.Fatalf("nfsdPortlistHasTCP2049(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func healthyRootRuntimeRead(path string) ([]byte, error) {
	switch path {
	case nfsdThreadsPath:
		return []byte("8\n"), nil
	case nfsdPortlistPath:
		return []byte("tcp 2049\n"), nil
	default:
		return nil, os.ErrNotExist
	}
}

type fakeRootFileInfo struct{ dir bool }

func (f fakeRootFileInfo) Name() string { return "root" }
func (f fakeRootFileInfo) Size() int64  { return 0 }
func (f fakeRootFileInfo) Mode() fs.FileMode {
	if f.dir {
		return fs.ModeDir
	}
	return 0
}
func (f fakeRootFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeRootFileInfo) IsDir() bool        { return f.dir }
func (f fakeRootFileInfo) Sys() any           { return nil }
