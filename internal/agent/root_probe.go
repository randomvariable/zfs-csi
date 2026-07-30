// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	nfsdThreadsPath  = "/proc/fs/nfsd/threads"
	nfsdPortlistPath = "/proc/fs/nfsd/portlist"
)

var statRootRuntimePath = os.Stat
var readRootRuntimePath = os.ReadFile

// probeNFSDRuntime validates only pre-auth structural/runtime prerequisites.
// It must not ask /proc/fs/nfsd/filehandle to resolve domain "*": that domain
// legitimately does not exist until an authorized auth.unix.ip upcall arrives.
func probeNFSDRuntime(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return newRootPreflightRetryable(err)
	}
	for _, path := range []string{root, nfsdThreadsPath, nfsdPortlistPath} {
		info, err := statRootRuntimePath(path)
		if err != nil {
			return classifyRootRuntimeStat(path, err)
		}
		if path == root && !info.IsDir() {
			return newRootPreflightTerminalConfig(fmt.Errorf("NFS root %s is not a directory", root))
		}
	}
	threads, err := readRootRuntimePath(nfsdThreadsPath)
	if err != nil {
		return newRootPreflightRetryable(fmt.Errorf("read NFS runtime path %s: %w", nfsdThreadsPath, err))
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(threads)))
	if err != nil || n <= 0 {
		return newRootPreflightRetryable(fmt.Errorf("nfsd threads not positive: %q (%v)", threads, err))
	}
	ports, err := readRootRuntimePath(nfsdPortlistPath)
	if err != nil {
		return newRootPreflightRetryable(fmt.Errorf("read NFS runtime path %s: %w", nfsdPortlistPath, err))
	}
	if !nfsdPortlistHasTCP2049(ports) {
		return newRootPreflightRetryable(fmt.Errorf("nfsd portlist lacks 2049: %q", ports))
	}
	return nil
}

func nfsdPortlistHasTCP2049(portlist []byte) bool {
	for _, line := range strings.Split(string(portlist), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "tcp" && fields[1] == "2049" {
			return true
		}
	}
	return false
}

func classifyRootRuntimeStat(path string, err error) error {
	wrapped := fmt.Errorf("stat NFS runtime path %s: %w", path, err)
	switch {
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return newRootPreflightTerminalDeploy(wrapped)
	default:
		return newRootPreflightRetryable(wrapped)
	}
}
