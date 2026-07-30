//go:build linux

// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package psk

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	keyPerm = 0x3f3f0000 // KEY_POS_ALL | KEY_USR_ALL
)

// ErrNVMEKeyringAbsent means the kernel's .nvme root keyring could not be
// found. This includes an unreadable /proc/keys fallback: callers must fail
// closed rather than assume a different keyring namespace is usable.
var ErrNVMEKeyringAbsent = errors.New("psk: .nvme keyring absent or unreadable")

type keyringOps struct {
	search func(int, string, string, int) (int, error)
	add    func(string, string, []byte, int) (int, error)
	intctl func(int, int, int, int, int) (int, error)
	keys   func() (*os.File, error)
}

var linuxKeyringOps = keyringOps{
	search: unix.KeyctlSearch,
	add:    unix.AddKey,
	intctl: unix.KeyctlInt,
	keys: func() (*os.File, error) {
		return os.Open("/proc/keys")
	},
}

// NVMEKeyringID finds the kernel's .nvme root ring. The psk key type is
// KEY_TYPE_NET_DOMAIN, so NVMe consumes it from init_net; callers must run in
// the host network namespace before calling this package's Linux adapter.
func NVMEKeyringID() (int, error) {
	return nvmeKeyringID(linuxKeyringOps)
}

func nvmeKeyringID(ops keyringOps) (int, error) {
	if id, err := ops.search(unix.KEY_SPEC_SESSION_KEYRING, "keyring", ".nvme", 0); err == nil {
		return id, nil
	} else if !ringFallbackNeeded(err) {
		return 0, fmt.Errorf("psk: search session .nvme keyring: %w", err)
	}

	id, err := findNVMEKeyring(ops.keys)
	if err != nil {
		return 0, err
	}
	// Linking makes this use available to descendants. Discovery is still valid
	// when policy disallows the best-effort local convenience link.
	_, _ = ops.intctl(unix.KEYCTL_LINK, id, unix.KEY_SPEC_SESSION_KEYRING, 0, 0)
	return id, nil
}

func ringFallbackNeeded(err error) bool {
	return errors.Is(err, unix.ENOKEY) || errors.Is(err, unix.EKEYEXPIRED) || errors.Is(err, unix.EKEYREVOKED)
}

func findNVMEKeyring(open func() (*os.File, error)) (int, error) {
	f, err := open()
	if err != nil {
		return 0, ErrNVMEKeyringAbsent
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		id, ok := parseNVMEKeyringLine(s.Text())
		if ok {
			return id, nil
		}
	}
	return 0, ErrNVMEKeyringAbsent
}

func parseNVMEKeyringLine(line string) (int, bool) {
	fields := strings.Fields(line)
	// /proc/keys: serial flags usage timeout perms uid gid type description.
	if len(fields) < 9 || fields[7] != "keyring" {
		return 0, false
	}
	description := strings.Join(fields[8:], " ")
	if !isNVMEKeyringDescription(description) {
		return 0, false
	}
	id, err := strconv.ParseInt(fields[0], 16, 32)
	if err != nil {
		return 0, false
	}
	return int(id), true
}

func isNVMEKeyringDescription(description string) bool {
	if description == ".nvme" || description == ".nvme: empty" {
		return true
	}
	serial, ok := strings.CutPrefix(description, ".nvme: ")
	if !ok || serial == "" || serial[0] == '0' {
		return false
	}
	for _, c := range serial {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// Install derives and replaces a psk key under the .nvme ring.
// Version is explicit; callers pass DefaultVersion for Linux 6.8-safe v0.
func Install(ic Interchange, hostNQN, subsysNQN string, version int) (Material, error) {
	return install(linuxKeyringOps, ic, hostNQN, subsysNQN, version)
}

func install(ops keyringOps, ic Interchange, hostNQN, subsysNQN string, version int) (Material, error) {
	material, err := Derive(ic, hostNQN, subsysNQN, version)
	if err != nil {
		return Material{}, err
	}
	ringID, err := nvmeKeyringID(ops)
	if err != nil {
		return Material{}, err
	}
	if old, err := ops.search(ringID, "psk", material.Identity, 0); err == nil {
		if _, err := ops.intctl(unix.KEYCTL_REVOKE, old, 0, 0, 0); err != nil && !staleKey(err) {
			return Material{}, fmt.Errorf("psk: revoke identity %q in ring %d: %w", material.Identity, ringID, err)
		}
	} else if !errors.Is(err, unix.ENOKEY) {
		return Material{}, fmt.Errorf("psk: search identity %q in ring %d: %w", material.Identity, ringID, err)
	}
	keyID, err := ops.add("psk", material.Identity, material.TLSPSK, ringID)
	if err != nil {
		return Material{}, fmt.Errorf("psk: add identity %q in ring %d: %w", material.Identity, ringID, err)
	}
	if _, err := ops.intctl(unix.KEYCTL_SETPERM, keyID, keyPerm, 0, 0); err != nil {
		return Material{}, fmt.Errorf("psk: set permissions identity %q in ring %d: %w", material.Identity, ringID, err)
	}
	return material, nil
}

// Remove revokes then unlinks an installed key. Missing rings and keys are
// idempotent success; revoke always happens before unlink.
func Remove(identity string) error {
	return remove(linuxKeyringOps, identity)
}

func remove(ops keyringOps, identity string) error {
	ringID, err := nvmeKeyringID(ops)
	if errors.Is(err, ErrNVMEKeyringAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	keyID, err := ops.search(ringID, "psk", identity, 0)
	if staleKey(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("psk: search identity %q in ring %d: %w", identity, ringID, err)
	}
	if _, err := ops.intctl(unix.KEYCTL_REVOKE, keyID, 0, 0, 0); err != nil && !staleKey(err) {
		return fmt.Errorf("psk: revoke identity %q in ring %d: %w", identity, ringID, err)
	}
	if _, err := ops.intctl(unix.KEYCTL_UNLINK, keyID, ringID, 0, 0); err != nil && !staleKey(err) {
		return fmt.Errorf("psk: unlink identity %q from ring %d: %w", identity, ringID, err)
	}
	return nil
}

func staleKey(err error) bool {
	return errors.Is(err, unix.ENOKEY) || errors.Is(err, unix.EKEYREVOKED)
}
