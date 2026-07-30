// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

// Package reachability validates and selects storage serving endpoints and
// consumer network domains.
package reachability

import (
	"fmt"
	"net"
	"path"
	"slices"
	"strconv"
	"strings"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

const (
	// TopologyKeyNetworkDomain is the stable CSI topology segment used for
	// worker-to-storage reachability. Segment values are operator-authored
	// Kubernetes label values, never endpoint addresses.
	TopologyKeyNetworkDomain = "topology.zfs.csi.randomvariable.co.uk/network-domain"
	DefaultNFSServicePort    = int32(2049)
	DefaultNVMeTCPPort       = int32(4420)
)

// ValidateStorageNodeEndpoints verifies status before inventory can become
// placement eligible. Both protocols are required because StorageNode status
// does not advertise per-pool protocol capabilities.
func ValidateStorageNodeEndpoints(node *zfscsiv1.StorageNode) error {
	if node.Spec.NetworkDomain == "" {
		return fmt.Errorf("StorageNode %q has empty networkDomain", node.Name)
	}
	if len(node.Status.ReachableFrom) == 0 {
		return fmt.Errorf("StorageNode %q has no reachableFrom domains", node.Name)
	}
	seenDomains := make(map[string]struct{}, len(node.Status.ReachableFrom))
	for _, domain := range node.Status.ReachableFrom {
		if domain == "" {
			return fmt.Errorf("StorageNode %q has empty reachableFrom domain", node.Name)
		}
		if _, duplicate := seenDomains[domain]; duplicate {
			return fmt.Errorf("StorageNode %q has duplicate reachableFrom domain %q", node.Name, domain)
		}
		seenDomains[domain] = struct{}{}
	}
	if _, ok := seenDomains[node.Spec.NetworkDomain]; !ok {
		return fmt.Errorf("StorageNode %q networkDomain %q is not reachableFrom itself", node.Name, node.Spec.NetworkDomain)
	}
	if _, err := SelectEndpoint(node.Status.Endpoints, zfscsiv1.StorageProtocolNFS); err != nil {
		return fmt.Errorf("StorageNode %q NFS endpoint: %w", node.Name, err)
	}
	if _, err := SelectEndpoint(node.Status.Endpoints, zfscsiv1.StorageProtocolNVMeTCP); err != nil {
		return fmt.Errorf("StorageNode %q NVMe-TCP endpoint: %w", node.Name, err)
	}
	return nil
}

// SelectEndpoint returns the stable first canonical endpoint for one protocol.
func SelectEndpoint(endpoints []zfscsiv1.StorageNodeEndpoint, protocol zfscsiv1.StorageProtocol) (zfscsiv1.StorageNodeEndpoint, error) {
	matches := make([]zfscsiv1.StorageNodeEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Protocol != protocol {
			continue
		}
		if err := ValidateEndpoint(endpoint); err != nil {
			return zfscsiv1.StorageNodeEndpoint{}, err
		}
		matches = append(matches, endpoint)
	}
	if len(matches) == 0 {
		return zfscsiv1.StorageNodeEndpoint{}, fmt.Errorf("missing %s endpoint", protocol)
	}
	slices.SortFunc(matches, func(a, b zfscsiv1.StorageNodeEndpoint) int {
		if c := strings.Compare(a.Host, b.Host); c != 0 {
			return c
		}
		return int(a.Port - b.Port)
	})
	return matches[0], nil
}

// ValidateEndpoint verifies a host and protocol port without accepting an
// embedded host:port pair. IPv6 literals are stored unbracketed in the API.
func ValidateEndpoint(endpoint zfscsiv1.StorageNodeEndpoint) error {
	if endpoint.Protocol != zfscsiv1.StorageProtocolNFS && endpoint.Protocol != zfscsiv1.StorageProtocolNVMeTCP {
		return fmt.Errorf("unsupported protocol %q", endpoint.Protocol)
	}
	if err := ValidateHost(endpoint.Host); err != nil {
		return err
	}
	if endpoint.Port < 1 || endpoint.Port > 65535 {
		return fmt.Errorf("endpoint port %d is outside 1..65535", endpoint.Port)
	}
	return nil
}

// ValidateHost accepts DNS names and IPv4/IPv6 literals, but not brackets,
// zones, paths, whitespace, or embedded ports.
func ValidateHost(host string) error {
	if host == "" || strings.TrimSpace(host) != host || strings.ContainsAny(host, "[]/%") {
		return fmt.Errorf("invalid endpoint host %q", host)
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if len(host) > 253 {
		return fmt.Errorf("endpoint host exceeds DNS length")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("invalid endpoint host %q", host)
		}
		for _, ch := range label {
			if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' {
				return fmt.Errorf("invalid endpoint host %q", host)
			}
		}
	}
	return nil
}

// JoinPortal renders the one accepted host:port boundary form. IPv6 literals
// are bracketed by net.JoinHostPort.
func JoinPortal(host string, port int32) (string, error) {
	endpoint := zfscsiv1.StorageNodeEndpoint{Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: host, Port: port}
	if err := ValidateEndpoint(endpoint); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}

// ParsePortal validates one canonical host:port endpoint and returns host/port.
func ParsePortal(portal string) (string, int32, error) {
	host, rawPort, err := net.SplitHostPort(portal)
	if err != nil {
		return "", 0, fmt.Errorf("split portal %q: %w", portal, err)
	}
	parsed, err := strconv.ParseInt(rawPort, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("parse portal port %q: %w", rawPort, err)
	}
	endpoint := zfscsiv1.StorageNodeEndpoint{Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: host, Port: int32(parsed)}
	if err := ValidateEndpoint(endpoint); err != nil {
		return "", 0, err
	}
	canonical, err := JoinPortal(host, int32(parsed))
	if err != nil || canonical != portal {
		return "", 0, fmt.Errorf("portal %q is not canonical", portal)
	}
	return host, int32(parsed), nil
}

// NFSMountSource formats a persisted NFS host and absolute export path for
// mount.nfs. IPv6 literals require brackets here, unlike OpenZFS sharenfs CIDRs.
func NFSMountSource(host, exportPath string) (string, error) {
	if err := ValidateHost(host); err != nil {
		return "", err
	}
	if exportPath == "" || !path.IsAbs(exportPath) || path.Clean(exportPath) != exportPath {
		return "", fmt.Errorf("invalid NFS export path %q", exportPath)
	}
	if net.ParseIP(host) != nil && strings.Contains(host, ":") {
		return "[" + host + "]:" + exportPath, nil
	}
	return host + ":" + exportPath, nil
}

// NFSClientPath translates a host-visible export path into the path visible
// below an NFSv4 pseudoroot. Both inputs must be canonical absolute paths.
func NFSClientPath(hostPath, rootPath string) (string, error) {
	if hostPath == "" || !path.IsAbs(hostPath) || path.Clean(hostPath) != hostPath {
		return "", fmt.Errorf("invalid NFS host path %q", hostPath)
	}
	if rootPath == "" || !path.IsAbs(rootPath) || path.Clean(rootPath) != rootPath {
		return "", fmt.Errorf("invalid NFS root path %q", rootPath)
	}
	if hostPath == rootPath {
		return "/", nil
	}
	if rootPath == "/" {
		return hostPath, nil
	}
	prefix := rootPath + "/"
	if !strings.HasPrefix(hostPath, prefix) {
		return "", fmt.Errorf("NFS host path %q is outside root %q", hostPath, rootPath)
	}
	return strings.TrimPrefix(hostPath, rootPath), nil
}

// NFSMountHost renders only the host component accepted by mount.nfs source
// syntax. It is useful when a stage contract carries host and path separately.
func NFSMountHost(host string) (string, error) {
	if err := ValidateHost(host); err != nil {
		return "", err
	}
	if net.ParseIP(host) != nil && strings.Contains(host, ":") {
		return "[" + host + "]", nil
	}
	return host, nil
}

// DomainOrder returns requisite domains in mandatory order, then preferred
// domains that are also requisite. Empty requisite means no domain constraint;
// preferred remains advisory ranking only.
func DomainOrder(requisite, preferred []string) ([]string, bool) {
	if len(requisite) == 0 {
		return slices.Clone(preferred), false
	}
	requisiteSet := make(map[string]struct{}, len(requisite))
	for _, domain := range requisite {
		requisiteSet[domain] = struct{}{}
	}
	result := make([]string, 0, len(requisite))
	seen := make(map[string]struct{}, len(requisite))
	for _, domain := range preferred {
		if _, required := requisiteSet[domain]; !required {
			continue
		}
		if _, duplicate := seen[domain]; !duplicate {
			result = append(result, domain)
			seen[domain] = struct{}{}
		}
	}
	for _, domain := range requisite {
		if _, duplicate := seen[domain]; !duplicate {
			result = append(result, domain)
			seen[domain] = struct{}{}
		}
	}
	return result, true
}
