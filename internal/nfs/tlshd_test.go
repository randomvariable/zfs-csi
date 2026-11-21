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

package nfs

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCheckHandshakeListeners(t *testing.T) {
	originalLookup, originalCount := lookupTLSHandshakeFamily, countHandshakeListeners
	t.Cleanup(func() { lookupTLSHandshakeFamily, countHandshakeListeners = originalLookup, originalCount })

	family := genericNetlinkFamily{id: 30, groups: map[string]uint32{handshakeListenerGroup: 21}}
	for _, tc := range []struct {
		name      string
		family    genericNetlinkFamily
		lookupErr error
		listeners int
		want      int
		wantErr   string
	}{
		{name: "one daemon", family: family, listeners: 1, want: 1},
		{name: "no daemon before start", family: family, listeners: 0, want: 0},
		{name: "host daemon beside the CSI daemon", family: family, listeners: 2, want: 1, wantErr: "systemctl mask --now tlshd"},
		{name: "host daemon before start", family: family, listeners: 1, want: 0, wantErr: "pgrep -a tlshd"},
		{name: "daemon exited", family: family, listeners: 0, want: 1, wantErr: "cannot mount"},
		{name: "kernel without handshake support", lookupErr: unix.ENOENT, want: 1},
		{name: "family without listener group", family: genericNetlinkFamily{id: 30, groups: map[string]uint32{}}, want: 1, wantErr: "multicast group"},
		{name: "lookup failure", lookupErr: errors.New("boom"), want: 1, wantErr: "boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookupTLSHandshakeFamily = func(string) (genericNetlinkFamily, error) { return tc.family, tc.lookupErr }
			countHandshakeListeners = func(protocol int, group uint32) (int, error) {
				if protocol != unix.NETLINK_GENERIC || group != 21 {
					t.Fatalf("counted protocol %d group %d, want generic netlink group 21", protocol, group)
				}
				return tc.listeners, nil
			}
			err := CheckHandshakeListeners(tc.want)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckHandshakeListeners(%d) = %v, want nil", tc.want, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("CheckHandshakeListeners(%d) = %v, want error containing %q", tc.want, err, tc.wantErr)
			}
		})
	}
}

// TestCountNetlinkGroupListenersCountsLiveSubscribers joins the nlctrl
// "notify" group, which needs no privilege, and checks that the kernel's
// family and sock_diag replies decode to the subscribed sockets.
func TestCountNetlinkGroupListenersCountsLiveSubscribers(t *testing.T) {
	nlctrl, err := lookupGenericNetlinkFamily("nlctrl")
	if err != nil {
		t.Fatalf("look up nlctrl: %v", err)
	}
	group, ok := nlctrl.groups["notify"]
	if !ok {
		t.Fatalf("nlctrl groups = %v, want notify", nlctrl.groups)
	}

	counters := map[string]func() (int, error){
		"netlink_diag": func() (int, error) { return countNetlinkDiagGroupListeners(unix.NETLINK_GENERIC, group) },
		"/proc/net/netlink": func() (int, error) {
			return countProcNetlinkGroupListeners("/proc/net/netlink", unix.NETLINK_GENERIC, group)
		},
	}
	before := map[string]int{}
	for name, count := range counters {
		n, err := count()
		if name == "netlink_diag" && errors.Is(err, unix.ENOENT) {
			t.Logf("kernel cannot load netlink_diag; checking the /proc fallback only")
			delete(counters, name)
			continue
		}
		if err != nil {
			t.Fatalf("%s: count listeners: %v", name, err)
		}
		before[name] = n
	}
	for range 2 {
		fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_GENERIC)
		if err != nil {
			t.Fatalf("open generic-netlink socket: %v", err)
		}
		t.Cleanup(func() { _ = unix.Close(fd) })
		// /proc/net/netlink lists bound sockets only; tlshd binds through libnl.
		if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
			t.Fatalf("bind generic-netlink socket: %v", err)
		}
		if err := unix.SetsockoptInt(fd, unix.SOL_NETLINK, unix.NETLINK_ADD_MEMBERSHIP, int(group)); err != nil {
			t.Fatalf("join nlctrl notify group %d: %v", group, err)
		}
	}
	for name, count := range counters {
		after, err := count()
		if err != nil {
			t.Fatalf("%s: count listeners: %v", name, err)
		}
		if after-before[name] != 2 {
			t.Errorf("%s: listeners went from %d to %d after two joins, want +2", name, before[name], after)
		}
	}
}

func TestGroupBitSet(t *testing.T) {
	if bits.UintSize != 64 {
		t.Skip("bitmap fixture uses 64-bit unsigned longs")
	}
	// Two native-endian unsigned longs, as NETLINK_DIAG_GROUPS carries them,
	// with group g at bit g-1.
	words := []uint64{1<<0 | 1<<20, 1 << (69 - 64)}
	bitmap := make([]byte, 8*len(words))
	for i, word := range words {
		binary.NativeEndian.PutUint64(bitmap[8*i:], word)
	}
	for group, want := range map[uint32]bool{0: false, 1: true, 2: false, 21: true, 64: false, 70: true, 129: false} {
		if got := groupBitSet(bitmap, group); got != want {
			t.Errorf("groupBitSet(group %d) = %t, want %t", group, got, want)
		}
	}
}
