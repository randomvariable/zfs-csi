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
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	// handshakeListenerGroup is the handshake family's multicast group that
	// tlshd joins to receive handshake requests (HANDSHAKE_MCGRP_TLSHD).
	handshakeListenerGroup = "tlshd"

	// linux/netlink_diag.h. x/sys/unix does not define these.
	sizeofNetlinkDiagReq  = 20
	sizeofNetlinkDiagMsg  = 28
	netlinkDiagShowGroups = 0x2 // NDIAG_SHOW_GROUPS
	netlinkDiagGroups     = 1   // NETLINK_DIAG_GROUPS
)

var countHandshakeListeners = countNetlinkGroupListeners

// CheckHandshakeListeners fails unless exactly want daemons listen for kernel
// TLS handshake requests in the caller's network namespace.
//
// The kernel offers each NFS or NVMe/TCP TLS handshake to every listener in
// the namespace and hands it to the first one that accepts it. A second
// daemon, typically a host tlshd.service beside a hostNetwork zfs-csi pod,
// has none of the zfs-csi credentials, so every handshake it wins fails and
// the mount reports "access denied by server". A kernel without the handshake
// family starts no TLS handshakes, so it passes for any want.
func CheckHandshakeListeners(want int) error {
	family, err := lookupTLSHandshakeFamily(handshakeNetlinkFamily)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("look up generic-netlink family %q: %w", handshakeNetlinkFamily, err)
	}
	group, ok := family.groups[handshakeListenerGroup]
	if !ok {
		return fmt.Errorf("generic-netlink family %q has no %q multicast group", handshakeNetlinkFamily, handshakeListenerGroup)
	}
	got, err := countHandshakeListeners(unix.NETLINK_GENERIC, group)
	if err != nil {
		return fmt.Errorf("count kernel TLS handshake listeners: %w", err)
	}
	switch {
	case got > want:
		return fmt.Errorf("%d tlshd daemons listen for kernel TLS handshakes in this network namespace, want %d: "+
			"each handshake goes to the first daemon that accepts it, and one without the zfs-csi credentials fails it "+
			"(mount: access denied by server). Find the extra daemon with `pgrep -a tlshd` on the node; "+
			"stop a host tlshd with `systemctl mask --now tlshd`", got, want)
	case got < want:
		return fmt.Errorf("%d tlshd daemons listen for kernel TLS handshakes in this network namespace, want %d: "+
			"TLS volumes cannot mount until one does", got, want)
	}
	return nil
}

// countNetlinkGroupListeners counts the sockets of a netlink protocol in the
// caller's network namespace that joined multicast group.
//
// It asks netlink_diag, which reports every group. Without netlink_diag, for
// example after a kernel package upgrade removed the running kernel's
// modules, it reads /proc/net/netlink, which shows groups 1 to 32 only.
func countNetlinkGroupListeners(protocol int, group uint32) (int, error) {
	listeners, err := countNetlinkDiagGroupListeners(protocol, group)
	if errors.Is(err, unix.ENOENT) && group >= 1 && group <= 32 {
		return countProcNetlinkGroupListeners("/proc/net/netlink", protocol, group)
	}
	return listeners, err
}

// countProcNetlinkGroupListeners reads /proc/net/netlink, whose Groups column
// holds each socket's first 32 group bits.
func countProcNetlinkGroupListeners(path string, protocol int, group uint32) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	listeners := 0
	for _, line := range lines[1:] {
		// sk Eth Pid Groups Rmem Wmem Dump Locks Drops Inode
		fields := strings.Fields(line)
		if len(fields) < 4 {
			return 0, fmt.Errorf("malformed %s line %q", path, line)
		}
		eth, err := strconv.Atoi(fields[1])
		if err != nil {
			return 0, fmt.Errorf("malformed %s protocol %q: %w", path, fields[1], err)
		}
		groups, err := strconv.ParseUint(fields[3], 16, 32)
		if err != nil {
			return 0, fmt.Errorf("malformed %s groups %q: %w", path, fields[3], err)
		}
		if eth == protocol && groups&(1<<(group-1)) != 0 {
			listeners++
		}
	}
	return listeners, nil
}

func countNetlinkDiagGroupListeners(protocol int, group uint32) (int, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_SOCK_DIAG)
	if err != nil {
		return 0, fmt.Errorf("open sock_diag socket: %w", err)
	}
	defer unix.Close(fd)

	seq := netlinkSequence.Add(1)
	if err := unix.Sendto(fd, netlinkDiagRequest(protocol, seq), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return 0, fmt.Errorf("send netlink socket dump: %w", err)
	}

	// Keyed by socket inode: a single-protocol dump can report the same socket
	// more than once (seen on Linux 7.0).
	listeners := map[uint32]struct{}{}
	buffer := make([]byte, 64*1024)
	for {
		n, _, err := unix.Recvfrom(fd, buffer, 0)
		if err != nil {
			return 0, fmt.Errorf("receive netlink socket dump: %w", err)
		}
		done, err := parseNetlinkDiagListeners(buffer[:n], seq, group, listeners)
		if err != nil {
			return 0, err
		}
		if done {
			return len(listeners), nil
		}
	}
}

func netlinkDiagRequest(protocol int, seq uint32) []byte {
	request := make([]byte, unix.SizeofNlMsghdr+sizeofNetlinkDiagReq)
	binary.NativeEndian.PutUint32(request[0:4], uint32(len(request)))
	binary.NativeEndian.PutUint16(request[4:6], unix.SOCK_DIAG_BY_FAMILY)
	binary.NativeEndian.PutUint16(request[6:8], unix.NLM_F_REQUEST|unix.NLM_F_DUMP)
	binary.NativeEndian.PutUint32(request[8:12], seq)
	body := request[unix.SizeofNlMsghdr:]
	body[0] = unix.AF_NETLINK
	body[1] = byte(protocol)
	binary.NativeEndian.PutUint32(body[8:12], netlinkDiagShowGroups)
	return request
}

// parseNetlinkDiagListeners adds the inode of each socket in one sock_diag
// dump batch that joined group to listeners, and reports whether the batch
// ends the dump.
func parseNetlinkDiagListeners(response []byte, seq, group uint32, listeners map[uint32]struct{}) (done bool, err error) {
	for offset := 0; offset+unix.SizeofNlMsghdr <= len(response); {
		messageLength := int(binary.NativeEndian.Uint32(response[offset : offset+4]))
		if messageLength < unix.SizeofNlMsghdr || offset+messageLength > len(response) {
			return false, fmt.Errorf("malformed netlink response length %d", messageLength)
		}
		messageType := binary.NativeEndian.Uint16(response[offset+4 : offset+6])
		messageSeq := binary.NativeEndian.Uint32(response[offset+8 : offset+12])
		payload := response[offset+unix.SizeofNlMsghdr : offset+messageLength]
		offset += alignNetlink(messageLength)
		if messageSeq != seq {
			continue
		}
		switch messageType {
		case unix.NLMSG_DONE:
			return true, netlinkError(payload)
		case unix.NLMSG_ERROR:
			if err := netlinkError(payload); err != nil {
				return true, fmt.Errorf("netlink socket dump: %w", err)
			}
		case unix.SOCK_DIAG_BY_FAMILY:
			if len(payload) < sizeofNetlinkDiagMsg {
				return false, fmt.Errorf("malformed netlink_diag message length %d", len(payload))
			}
			// struct netlink_diag_msg: ndiag_ino follows family, type,
			// protocol, state, portid, dst_portid and dst_group.
			inode := binary.NativeEndian.Uint32(payload[16:20])
			err := forEachNetlinkAttribute(payload[sizeofNetlinkDiagMsg:], func(attributeType uint16, value []byte) error {
				if attributeType == netlinkDiagGroups && groupBitSet(value, group) {
					listeners[inode] = struct{}{}
				}
				return nil
			})
			if err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

// groupBitSet reports whether group is set in a kernel netlink group bitmap:
// an array of native-endian unsigned longs with group g at bit g-1.
func groupBitSet(bitmap []byte, group uint32) bool {
	if group == 0 {
		return false
	}
	bit := int(group - 1)
	const wordBytes = bits.UintSize / 8
	start := bit / bits.UintSize * wordBytes
	if start+wordBytes > len(bitmap) {
		return false
	}
	var word uint64
	if wordBytes == 8 {
		word = binary.NativeEndian.Uint64(bitmap[start:])
	} else {
		word = uint64(binary.NativeEndian.Uint32(bitmap[start:]))
	}
	return word&(1<<(bit%bits.UintSize)) != 0
}
