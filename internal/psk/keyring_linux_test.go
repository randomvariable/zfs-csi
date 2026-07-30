//go:build linux

// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package psk

import (
	"errors"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestParseNVMEKeyringLine(t *testing.T) {
	for _, tc := range []struct {
		line string
		id   int
		ok   bool
	}{
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme: empty", 0x123abcd, true},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme: 1", 0x123abcd, true},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme", 0x123abcd, true},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme child", 0, false},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme:evil", 0, false},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme: 0", 0, false},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvme: 12 nope", 0, false},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 keyring .nvmeevil", 0, false},
		{"0123abcd I------ 1 perm 3f3f0000 0 0 user .nvme", 0, false},
	} {
		id, ok := parseNVMEKeyringLine(tc.line)
		if id != tc.id || ok != tc.ok {
			t.Fatalf("parseNVMEKeyringLine(%q) = %x, %t", tc.line, id, ok)
		}
	}
}

func TestNVMEKeyringIDSearchAndFallback(t *testing.T) {
	linked := false
	ops := keyringOps{
		search: func(int, string, string, int) (int, error) { return 42, nil },
		intctl: func(cmd, _, _, _, _ int) (int, error) {
			linked = true
			if cmd != unix.KEYCTL_LINK {
				t.Fatalf("cmd = %d", cmd)
			}
			return 0, nil
		},
	}
	id, err := nvmeKeyringID(ops)
	if err != nil || id != 42 || linked {
		t.Fatalf("search result = %d, %v, linked=%t", id, err, linked)
	}

	ops.search = func(int, string, string, int) (int, error) { return 0, unix.ENOKEY }
	ops.keys = tempKeys(t, "0000002a I------ 1 perm 3f3f0000 0 0 keyring .nvme: empty\n")
	id, err = nvmeKeyringID(ops)
	if err != nil || id != 42 || !linked {
		t.Fatalf("fallback result = %d, %v, linked=%t", id, err, linked)
	}

	ops.keys = func() (*os.File, error) { return nil, unix.EACCES }
	if _, err := nvmeKeyringID(ops); !errors.Is(err, ErrNVMEKeyringAbsent) {
		t.Fatalf("unreadable fallback = %v", err)
	}
}

func TestInstallAndRemoveOrder(t *testing.T) {
	var calls []string
	searches := 0
	ops := keyringOps{
		search: func(ring int, typ, desc string, _ int) (int, error) {
			searches++
			if ring == unix.KEY_SPEC_SESSION_KEYRING {
				return 9, nil
			}
			if searches == 2 {
				return 17, nil
			}
			return 18, nil
		},
		add: func(typ, desc string, payload []byte, ring int) (int, error) {
			if typ != "psk" || ring != 9 || len(payload) != 32 || strings.Contains(desc, string(payload)) {
				t.Fatal("bad add arguments")
			}
			calls = append(calls, "add")
			return 18, nil
		},
		intctl: func(cmd, key, ring, _, _ int) (int, error) {
			switch cmd {
			case unix.KEYCTL_REVOKE:
				calls = append(calls, "revoke")
			case unix.KEYCTL_SETPERM:
				if key != 18 || ring != keyPerm {
					t.Fatal("bad setperm")
				}
				calls = append(calls, "setperm")
			case unix.KEYCTL_UNLINK:
				if ring != 9 {
					t.Fatal("bad unlink ring")
				}
				calls = append(calls, "unlink")
			}
			return 0, nil
		},
	}
	material, err := install(ops, Interchange{HMAC: HMACSHA256, Key: key32}, host, sub, DefaultVersion)
	if err != nil || material.Version != Version0 {
		t.Fatalf("install = %#v, %v", material, err)
	}
	if got := strings.Join(calls, ","); got != "revoke,add,setperm" {
		t.Fatalf("install order = %s", got)
	}
	calls = nil
	if err := remove(ops, material.Identity); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(calls, ","); got != "revoke,unlink" {
		t.Fatalf("remove order = %s", got)
	}
}

func TestInstallContinuesWhenStaleKeyWasRevoked(t *testing.T) {
	var added, permissions bool
	ops := keyringOps{
		search: func(ring int, _, _ string, _ int) (int, error) {
			if ring == unix.KEY_SPEC_SESSION_KEYRING {
				return 9, nil
			}
			return 17, nil
		},
		add: func(string, string, []byte, int) (int, error) {
			added = true
			return 18, nil
		},
		intctl: func(cmd, _, _, _, _ int) (int, error) {
			switch cmd {
			case unix.KEYCTL_REVOKE:
				return 0, unix.EKEYREVOKED
			case unix.KEYCTL_SETPERM:
				permissions = true
			}
			return 0, nil
		},
	}
	if _, err := install(ops, Interchange{HMAC: HMACSHA256, Key: key32}, host, sub, DefaultVersion); err != nil {
		t.Fatalf("install after revoked stale key = %v", err)
	}
	if !added || !permissions {
		t.Fatalf("install did not add/set permissions: add=%t setperm=%t", added, permissions)
	}
}

func TestInstallAndRemoveErrors(t *testing.T) {
	denied := keyringOps{search: func(int, string, string, int) (int, error) { return 0, unix.EPERM }}
	if _, err := install(denied, Interchange{HMAC: HMACSHA256, Key: key32}, host, sub, DefaultVersion); !errors.Is(err, unix.EPERM) {
		t.Fatalf("search EPERM = %v", err)
	}
	absent := keyringOps{search: func(int, string, string, int) (int, error) { return 0, unix.ENOKEY }, keys: func() (*os.File, error) { return nil, unix.EACCES }}
	if err := remove(absent, "NVMe0R01 h s"); err != nil {
		t.Fatalf("remove absent ring = %v", err)
	}

	ops := keyringOps{
		search: func(ring int, _, _ string, _ int) (int, error) {
			if ring == unix.KEY_SPEC_SESSION_KEYRING {
				return 9, nil
			}
			return 0, unix.ENOKEY
		},
	}
	if err := remove(ops, "NVMe0R01 h s"); err != nil {
		t.Fatalf("remove missing key = %v", err)
	}
}

func TestRemoveContinuesAfterStaleRevokeAndUnlink(t *testing.T) {
	var commands []int
	ops := keyringOps{
		search: func(ring int, _, _ string, _ int) (int, error) {
			if ring == unix.KEY_SPEC_SESSION_KEYRING {
				return 9, nil
			}
			return 17, nil
		},
		intctl: func(cmd, _, _, _, _ int) (int, error) {
			commands = append(commands, cmd)
			switch cmd {
			case unix.KEYCTL_REVOKE:
				return 0, unix.EKEYREVOKED
			case unix.KEYCTL_UNLINK:
				return 0, unix.ENOKEY
			}
			return 0, nil
		},
	}
	if err := remove(ops, "NVMe0R01 h s"); err != nil {
		t.Fatalf("remove stale key = %v", err)
	}
	if len(commands) != 2 || commands[0] != unix.KEYCTL_REVOKE || commands[1] != unix.KEYCTL_UNLINK {
		t.Fatalf("remove commands = %v", commands)
	}
}

func tempKeys(t *testing.T, content string) func() (*os.File, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "keys")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return func() (*os.File, error) { return os.Open(f.Name()) }
}
