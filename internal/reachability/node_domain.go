// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package reachability

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	NodeDomainSourceStatic    = "static"
	NodeDomainSourceNodeLabel = "nodeLabel"
	DefaultNodeDomainLabelKey = TopologyKeyNetworkDomain
)

// NodeGetter is deliberately narrower than a Kubernetes client. Label mode can
// perform one Node GET, but cannot list or watch Nodes.
type NodeGetter interface {
	GetNode(ctx context.Context, name string) (*corev1.Node, error)
}

// NodeDomainConfig selects static compatibility or one startup Node-label GET.
type NodeDomainConfig struct {
	Source       string
	StaticDomain string
	LabelKey     string
	NodeName     string
}

// ResolveNodeDomain returns one validated CSI topology segment value.
func ResolveNodeDomain(ctx context.Context, cfg NodeDomainConfig, getter NodeGetter) (string, error) {
	switch cfg.Source {
	case "", NodeDomainSourceStatic:
		if cfg.StaticDomain == "" {
			return "", fmt.Errorf("static network domain is required")
		}
		if err := ValidateDomain(cfg.StaticDomain); err != nil {
			return "", err
		}
		return cfg.StaticDomain, nil
	case NodeDomainSourceNodeLabel:
		if cfg.StaticDomain != "" {
			return "", fmt.Errorf("static network domain conflicts with node-label mode")
		}
		if cfg.NodeName == "" {
			return "", fmt.Errorf("NODE_NAME is required in node-label mode")
		}
		labelKey := cfg.LabelKey
		if labelKey == "" {
			labelKey = DefaultNodeDomainLabelKey
		}
		if problems := validation.IsQualifiedName(labelKey); len(problems) != 0 {
			return "", fmt.Errorf("invalid network domain label key %q: %v", labelKey, problems)
		}
		if getter == nil {
			return "", fmt.Errorf("Node getter is required in node-label mode")
		}
		node, err := getter.GetNode(ctx, cfg.NodeName)
		if err != nil {
			return "", fmt.Errorf("get Kubernetes Node %q: %w", cfg.NodeName, err)
		}
		if node == nil {
			return "", fmt.Errorf("get Kubernetes Node %q returned nil", cfg.NodeName)
		}
		if node.Name != cfg.NodeName {
			return "", fmt.Errorf("get Kubernetes Node %q returned %q", cfg.NodeName, node.Name)
		}
		domain, exists := node.Labels[labelKey]
		if !exists {
			return "", fmt.Errorf("Kubernetes Node %q is missing label %q", cfg.NodeName, labelKey)
		}
		if domain == "" {
			return "", fmt.Errorf("Kubernetes Node %q label %q is empty", cfg.NodeName, labelKey)
		}
		if err := ValidateDomain(domain); err != nil {
			return "", fmt.Errorf("Kubernetes Node %q label %q: %w", cfg.NodeName, labelKey, err)
		}
		return domain, nil
	default:
		return "", fmt.Errorf("unsupported network domain source %q", cfg.Source)
	}
}

// ValidateDomain applies Kubernetes label-value grammar to topology domains.
func ValidateDomain(domain string) error {
	if domain == "" {
		return fmt.Errorf("invalid empty network domain")
	}
	if problems := validation.IsValidLabelValue(domain); len(problems) != 0 {
		return fmt.Errorf("invalid network domain %q: %v", domain, problems)
	}
	return nil
}

// CanonicalDomains validates, de-duplicates and sorts a non-empty domain set.
func CanonicalDomains(domains []string) ([]string, error) {
	if len(domains) == 0 {
		return nil, fmt.Errorf("reachableFrom must be non-empty")
	}
	set := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		if err := ValidateDomain(domain); err != nil {
			return nil, err
		}
		set[domain] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for domain := range set {
		result = append(result, domain)
	}
	slices.Sort(result)
	return result, nil
}

// CanonicalReachableFrom also requires owner network-domain membership.
func CanonicalReachableFrom(networkDomain string, domains []string) ([]string, error) {
	if err := ValidateDomain(networkDomain); err != nil {
		return nil, err
	}
	canonical, err := CanonicalDomains(domains)
	if err != nil {
		return nil, err
	}
	if _, found := slices.BinarySearch(canonical, networkDomain); !found {
		return nil, fmt.Errorf("reachableFrom must include network domain %q", networkDomain)
	}
	return canonical, nil
}
