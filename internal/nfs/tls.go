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
	"fmt"
	"net"
	"strings"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const handshakeNetlinkFamily = "handshake"

var netlinkSequence atomic.Uint32

var lookupTLSHandshakeFamily = lookupGenericNetlinkFamily

// EnsureTLSRuntime verifies the host kernel registered the generic-netlink
// handshake family used by both NFS client and NFSD RPC-with-TLS upcalls.
// Missing support is a hard failure so a TLS Volume cannot become Ready on a
// plaintext-only server.
func EnsureTLSRuntime(endpoint string) error {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return fmt.Errorf("NFS endpoint is empty")
	}
	if net.ParseIP(endpoint) == nil {
		if strings.ContainsAny(endpoint, "[]/: ") {
			return fmt.Errorf("invalid NFS endpoint %q", endpoint)
		}
	}
	familyID, err := lookupTLSHandshakeFamily(handshakeNetlinkFamily)
	if err != nil {
		return fmt.Errorf("look up generic-netlink family %q: %w", handshakeNetlinkFamily, err)
	}
	if familyID == 0 {
		return fmt.Errorf("generic-netlink family %q returned invalid family ID 0", handshakeNetlinkFamily)
	}

	return nil
}

func lookupGenericNetlinkFamily(name string) (uint16, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_GENERIC)
	if err != nil {
		return 0, fmt.Errorf("open generic-netlink socket: %w", err)
	}
	defer unix.Close(fd)

	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return 0, fmt.Errorf("bind generic-netlink socket: %w", err)
	}

	seq := netlinkSequence.Add(1)
	request := genericNetlinkFamilyRequest(name, seq)
	if err := unix.Sendto(fd, request, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return 0, fmt.Errorf("send family lookup: %w", err)
	}

	buffer := make([]byte, 8192)
	n, _, err := unix.Recvfrom(fd, buffer, 0)
	if err != nil {
		return 0, fmt.Errorf("receive family lookup: %w", err)
	}
	return parseGenericNetlinkFamilyResponse(buffer[:n], seq)
}

func genericNetlinkFamilyRequest(name string, seq uint32) []byte {
	nameBytes := append([]byte(name), 0)
	attributeLength := 4 + len(nameBytes)
	messageLength := unix.SizeofNlMsghdr + 4 + alignNetlink(attributeLength)
	message := make([]byte, messageLength)
	binary.NativeEndian.PutUint32(message[0:4], uint32(messageLength))
	binary.NativeEndian.PutUint16(message[4:6], unix.GENL_ID_CTRL)
	binary.NativeEndian.PutUint16(message[6:8], unix.NLM_F_REQUEST)
	binary.NativeEndian.PutUint32(message[8:12], seq)
	message[unix.SizeofNlMsghdr] = unix.CTRL_CMD_GETFAMILY
	message[unix.SizeofNlMsghdr+1] = 1
	offset := unix.SizeofNlMsghdr + 4
	binary.NativeEndian.PutUint16(message[offset:offset+2], uint16(attributeLength))
	binary.NativeEndian.PutUint16(message[offset+2:offset+4], unix.CTRL_ATTR_FAMILY_NAME)
	copy(message[offset+4:], nameBytes)
	return message
}

func parseGenericNetlinkFamilyResponse(response []byte, seq uint32) (uint16, error) {
	for offset := 0; offset+unix.SizeofNlMsghdr <= len(response); {
		messageLength := int(binary.NativeEndian.Uint32(response[offset : offset+4]))
		if messageLength < unix.SizeofNlMsghdr || offset+messageLength > len(response) {
			return 0, fmt.Errorf("malformed netlink response length %d", messageLength)
		}
		messageType := binary.NativeEndian.Uint16(response[offset+4 : offset+6])
		messageSeq := binary.NativeEndian.Uint32(response[offset+8 : offset+12])
		if messageSeq == seq {
			payload := response[offset+unix.SizeofNlMsghdr : offset+messageLength]
			if messageType == unix.NLMSG_ERROR {
				if len(payload) < 4 {
					return 0, fmt.Errorf("malformed netlink error response")
				}
				errno := int32(binary.NativeEndian.Uint32(payload[:4]))
				if errno != 0 {
					return 0, unix.Errno(-errno)
				}
			} else if messageType == unix.GENL_ID_CTRL {
				return parseFamilyIDAttributes(payload)
			}
		}
		offset += alignNetlink(messageLength)
	}
	return 0, fmt.Errorf("generic-netlink family response not found")
}

func parseFamilyIDAttributes(payload []byte) (uint16, error) {
	if len(payload) < 4 {
		return 0, fmt.Errorf("malformed generic-netlink response")
	}
	for offset := 4; offset+4 <= len(payload); {
		attributeLength := int(binary.NativeEndian.Uint16(payload[offset : offset+2]))
		attributeType := binary.NativeEndian.Uint16(payload[offset+2:offset+4]) &^ unix.NLA_F_NESTED
		if attributeLength < 4 || offset+attributeLength > len(payload) {
			return 0, fmt.Errorf("malformed generic-netlink attribute length %d", attributeLength)
		}
		if attributeType == unix.CTRL_ATTR_FAMILY_ID {
			if attributeLength < 6 {
				return 0, fmt.Errorf("malformed family ID attribute")
			}
			return binary.NativeEndian.Uint16(payload[offset+4 : offset+6]), nil
		}
		offset += alignNetlink(attributeLength)
	}
	return 0, fmt.Errorf("family ID attribute not found")
}

func alignNetlink(length int) int {
	return (length + 3) &^ 3
}
