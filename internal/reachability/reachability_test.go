// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package reachability

import (
	"reflect"
	"testing"
)

func TestDomainOrder(t *testing.T) {
	tests := []struct {
		name      string
		requisite []string
		preferred []string
		want      []string
		wantBound bool
	}{
		{
			name:      "preferred ranks requisite intersection then deterministic fallback",
			requisite: []string{"zone-a", "zone-b", "zone-c"},
			preferred: []string{"zone-c", "unreachable", "zone-a"},
			want:      []string{"zone-c", "zone-a", "zone-b"},
			wantBound: true,
		},
		{
			name:      "duplicates collapse when requisite constrains",
			requisite: []string{"zone-b", "zone-a", "zone-b"},
			preferred: []string{"zone-a", "zone-a", "zone-b"},
			want:      []string{"zone-a", "zone-b"},
			wantBound: true,
		},
		{
			name:      "preferred only remains advisory and preserves caller order",
			preferred: []string{"zone-b", "zone-a", "zone-b"},
			want:      []string{"zone-b", "zone-a", "zone-b"},
		},
		{
			name:      "invalid segment values are not validated by ordering helper",
			requisite: []string{"", "bad/value", "zone-a"},
			preferred: []string{"bad/value", "missing"},
			want:      []string{"bad/value", "", "zone-a"},
			wantBound: true,
		},
		{name: "empty requirements", want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, bound := DomainOrder(test.requisite, test.preferred)
			if !reflect.DeepEqual(got, test.want) || bound != test.wantBound {
				t.Fatalf("DomainOrder(%v, %v) = (%v, %t), want (%v, %t)", test.requisite, test.preferred, got, bound, test.want, test.wantBound)
			}
		})
	}
}

func TestNFSMountFormatting(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		exportPath string
		wantHost   string
		wantSource string
		wantErr    bool
	}{
		{name: "DNS", host: "storage-a.example.test", exportPath: "/tank/app", wantHost: "storage-a.example.test", wantSource: "storage-a.example.test:/tank/app"},
		{name: "IPv4", host: "192.0.2.10", exportPath: "/tank/app", wantHost: "192.0.2.10", wantSource: "192.0.2.10:/tank/app"},
		{name: "IPv6 uses CSI mount brackets", host: "fd00::1", exportPath: "/export/path", wantHost: "[fd00::1]", wantSource: "[fd00::1]:/export/path"},
		{name: "root export", host: "storage-a", exportPath: "/", wantHost: "storage-a", wantSource: "storage-a:/"},
		{name: "empty path", host: "storage-a", wantErr: true},
		{name: "relative path", host: "storage-a", exportPath: "tank/app", wantErr: true},
		{name: "unclean traversal", host: "storage-a", exportPath: "/tank/../etc", wantErr: true},
		{name: "unclean trailing slash", host: "storage-a", exportPath: "/tank/app/", wantErr: true},
		{name: "bracketed host belongs only in rendered source", host: "[fd00::1]", exportPath: "/tank/app", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host, hostErr := NFSMountHost(test.host)
			source, sourceErr := NFSMountSource(test.host, test.exportPath)
			if test.wantErr {
				if sourceErr == nil {
					t.Fatalf("NFSMountSource(%q, %q) = %q, want error", test.host, test.exportPath, source)
				}
				return
			}
			if hostErr != nil || sourceErr != nil {
				t.Fatalf("NFSMountHost/NFSMountSource errors = (%v, %v)", hostErr, sourceErr)
			}
			if host != test.wantHost || source != test.wantSource {
				t.Fatalf("NFS rendering = (%q, %q), want (%q, %q)", host, source, test.wantHost, test.wantSource)
			}
		})
	}

	// OpenZFS sharenfs consumes CIDRs/host ACLs separately; CSI source brackets
	// are introduced only at this mount.nfs boundary and are not endpoint data.
	if got, err := NFSMountHost("fd00::1"); err != nil || got == "fd00::1" {
		t.Fatalf("IPv6 mount host = %q, err=%v; want bracketed rendering distinct from raw sharenfs host", got, err)
	}
}

func TestNFSClientPath(t *testing.T) {
	tests := []struct {
		name     string
		hostPath string
		rootPath string
		want     string
		wantErr  bool
	}{
		{name: "root", hostPath: "/tank", rootPath: "/tank", want: "/"},
		{name: "descendant", hostPath: "/tank/csi/fs/pvc-x", rootPath: "/tank", want: "/csi/fs/pvc-x"},
		{name: "component boundary", hostPath: "/tank2/x", rootPath: "/tank", wantErr: true},
		{name: "outside root", hostPath: "/other/x", rootPath: "/tank", wantErr: true},
		{name: "relative host path", hostPath: "tank/csi/fs/pvc-x", rootPath: "/tank", wantErr: true},
		{name: "double slash host path", hostPath: "/tank//csi/fs/pvc-x", rootPath: "/tank", wantErr: true},
		{name: "dot host path", hostPath: "/tank/./csi/fs/pvc-x", rootPath: "/tank", wantErr: true},
		{name: "traversal host path", hostPath: "/tank/csi/../fs/pvc-x", rootPath: "/tank", wantErr: true},
		{name: "empty host path", rootPath: "/tank", wantErr: true},
		{name: "empty root path", hostPath: "/tank/csi/fs/pvc-x", wantErr: true},
		{name: "relative root path", hostPath: "/tank/csi/fs/pvc-x", rootPath: "tank", wantErr: true},
		{name: "unclean root path", hostPath: "/tank/csi/fs/pvc-x", rootPath: "/tank/.", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NFSClientPath(test.hostPath, test.rootPath)
			if test.wantErr {
				if err == nil {
					t.Fatalf("NFSClientPath(%q, %q) = %q, want error", test.hostPath, test.rootPath, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("NFSClientPath(%q, %q) = (%q, %v), want (%q, nil)", test.hostPath, test.rootPath, got, err, test.want)
			}
		})
	}
}

func TestPortalRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		host   string
		port   int32
		portal string
	}{
		{name: "DNS", host: "storage-a.example.test", port: 4420, portal: "storage-a.example.test:4420"},
		{name: "IPv4", host: "192.0.2.10", port: 4420, portal: "192.0.2.10:4420"},
		{name: "IPv6", host: "fd00::1", port: 4420, portal: "[fd00::1]:4420"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			portal, err := JoinPortal(test.host, test.port)
			if err != nil || portal != test.portal {
				t.Fatalf("JoinPortal(%q, %d) = %q, %v; want %q", test.host, test.port, portal, err, test.portal)
			}
			host, port, err := ParsePortal(portal)
			if err != nil || host != test.host || port != test.port {
				t.Fatalf("ParsePortal(%q) = (%q, %d, %v), want (%q, %d, nil)", portal, host, port, err, test.host, test.port)
			}
		})
	}
}

func TestPortalRejectsMalformedInput(t *testing.T) {
	for _, portal := range []string{
		"fd00::1:4420",        // IPv6 must be bracketed at the boundary.
		"[fd00::1]",           // Port required.
		"[fd00::1]:04420",     // Noncanonical decimal port.
		"[fd00::1%eth0]:4420", // Zones are not stable endpoint identity.
		"storage-a:0",
		"storage-a:65536",
		"storage-a:-1",
		"storage-a:not-a-port",
		"storage-a:4420/path",
		"storage-a.example:4420:1",
		" storage-a:4420",
		"storage-a :4420",
		"[not-ipv6]:4420",
	} {
		t.Run(portal, func(t *testing.T) {
			if host, port, err := ParsePortal(portal); err == nil {
				t.Fatalf("ParsePortal(%q) = (%q, %d, nil), want error", portal, host, port)
			}
		})
	}

	for _, input := range []struct {
		host string
		port int32
	}{
		{host: "[fd00::1]", port: 4420},
		{host: "fd00::1%eth0", port: 4420},
		{host: "storage-a:4420", port: 4420},
		{host: "storage-a/path", port: 4420},
		{host: "storage-a", port: 0},
		{host: "storage-a", port: 65536},
	} {
		if portal, err := JoinPortal(input.host, input.port); err == nil {
			t.Errorf("JoinPortal(%q, %d) = %q, nil; want error", input.host, input.port, portal)
		}
	}
}

func TestValidateHost(t *testing.T) {
	for _, host := range []string{"storage-a", "storage-a.example.test", "localhost", "192.0.2.1", "2001:db8::1"} {
		if err := ValidateHost(host); err != nil {
			t.Errorf("ValidateHost(%q) = %v, want nil", host, err)
		}
	}

	for _, host := range []string{
		"",
		" storage-a",
		"storage-a ",
		"storage_a",
		"-storage-a",
		"storage-a-",
		"storage..a",
		"storage/a",
		"storage%zone",
		"storage:4420",
		"[2001:db8::1]",
		"2001:db8::1%eth0",
		"storage\na",
	} {
		t.Run(host, func(t *testing.T) {
			if err := ValidateHost(host); err == nil {
				t.Fatalf("ValidateHost(%q) = nil, want error", host)
			}
		})
	}
}
