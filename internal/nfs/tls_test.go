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
	"testing"

	"golang.org/x/sys/unix"
)

func TestEnsureTLSRuntimeFailsClosed(t *testing.T) {
	original := lookupTLSHandshakeFamily
	t.Cleanup(func() { lookupTLSHandshakeFamily = original })

	for _, tc := range []struct {
		name     string
		endpoint string
		lookup   func(string) (uint16, error)
		wantErr  bool
	}{
		{name: "ready", endpoint: "192.0.2.10", lookup: func(string) (uint16, error) { return 42, nil }},
		{name: "missing family", endpoint: "storage.example", lookup: func(string) (uint16, error) { return 0, errors.New("not found") }, wantErr: true},
		{name: "invalid family ID", endpoint: "storage.example", lookup: func(string) (uint16, error) { return 0, nil }, wantErr: true},
		{name: "empty endpoint", endpoint: "", lookup: func(string) (uint16, error) { return 42, nil }, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookupTLSHandshakeFamily = tc.lookup
			err := EnsureTLSRuntime(tc.endpoint)
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsureTLSRuntime() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestParseGenericNetlinkFamilyResponse(t *testing.T) {
	const (
		familyID = uint16(37)
		seq      = uint32(9)
	)
	payload := make([]byte, 12)
	payload[0] = unix.CTRL_CMD_GETFAMILY
	payload[1] = 1
	binary.NativeEndian.PutUint16(payload[4:6], 6)
	binary.NativeEndian.PutUint16(payload[6:8], unix.CTRL_ATTR_FAMILY_ID)
	binary.NativeEndian.PutUint16(payload[8:10], familyID)
	response := make([]byte, unix.SizeofNlMsghdr+len(payload))
	binary.NativeEndian.PutUint32(response[0:4], uint32(len(response)))
	binary.NativeEndian.PutUint16(response[4:6], unix.GENL_ID_CTRL)
	binary.NativeEndian.PutUint32(response[8:12], seq)
	copy(response[unix.SizeofNlMsghdr:], payload)

	got, err := parseGenericNetlinkFamilyResponse(response, seq)
	if err != nil {
		t.Fatalf("parse family response: %v", err)
	}
	if got != familyID {
		t.Fatalf("family ID = %d, want %d", got, familyID)
	}
}

func TestParseGenericNetlinkFamilyResponseRejectsErrorsAndMalformedPayload(t *testing.T) {
	const seq = uint32(11)
	errorResponse := make([]byte, unix.SizeofNlMsghdr+4)
	binary.NativeEndian.PutUint32(errorResponse[0:4], uint32(len(errorResponse)))
	binary.NativeEndian.PutUint16(errorResponse[4:6], unix.NLMSG_ERROR)
	binary.NativeEndian.PutUint32(errorResponse[8:12], seq)
	errno := -int32(unix.ENOENT)
	binary.NativeEndian.PutUint32(errorResponse[unix.SizeofNlMsghdr:], uint32(errno))
	if _, err := parseGenericNetlinkFamilyResponse(errorResponse, seq); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("netlink error = %v, want ENOENT", err)
	}
	if _, err := parseGenericNetlinkFamilyResponse([]byte{1, 2, 3}, seq); err == nil {
		t.Fatal("short netlink response unexpectedly accepted")
	}
}
