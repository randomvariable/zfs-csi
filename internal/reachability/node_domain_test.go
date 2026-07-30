// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package reachability

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type countingNodeGetter struct {
	node  *corev1.Node
	err   error
	gets  int
	names []string
}

func (g *countingNodeGetter) GetNode(_ context.Context, name string) (*corev1.Node, error) {
	g.gets++
	g.names = append(g.names, name)
	return g.node, g.err
}

func TestResolveNodeDomainStaticCompatibility(t *testing.T) {
	g := &countingNodeGetter{}
	got, err := ResolveNodeDomain(t.Context(), NodeDomainConfig{
		Source:       NodeDomainSourceStatic,
		StaticDomain: "workers",
		NodeName:     "worker-a",
	}, g)
	if err != nil || got != "workers" {
		t.Fatalf("ResolveNodeDomain = %q, %v; want workers, nil", got, err)
	}
	if g.gets != 0 {
		t.Fatalf("static mode Node GETs = %d, want 0", g.gets)
	}
}

func TestResolveNodeDomainLabelGetsExactlyOnce(t *testing.T) {
	g := &countingNodeGetter{node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "worker-a",
		Labels: map[string]string{TopologyKeyNetworkDomain: "fabric-a"},
	}}}
	got, err := ResolveNodeDomain(t.Context(), NodeDomainConfig{
		Source:   NodeDomainSourceNodeLabel,
		LabelKey: TopologyKeyNetworkDomain,
		NodeName: "worker-a",
	}, g)
	if err != nil || got != "fabric-a" {
		t.Fatalf("ResolveNodeDomain = %q, %v; want fabric-a, nil", got, err)
	}
	if g.gets != 1 || len(g.names) != 1 || g.names[0] != "worker-a" {
		t.Fatalf("Node GET calls = %d names=%v, want exactly [worker-a]", g.gets, g.names)
	}
}

func TestResolveNodeDomainFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		cfg  NodeDomainConfig
		get  *countingNodeGetter
		want string
	}{
		{name: "unknown source", cfg: NodeDomainConfig{Source: "other", StaticDomain: "workers"}, get: &countingNodeGetter{}, want: "source"},
		{name: "empty static domain", cfg: NodeDomainConfig{Source: NodeDomainSourceStatic}, get: &countingNodeGetter{}, want: "network domain"},
		{name: "invalid static domain", cfg: NodeDomainConfig{Source: NodeDomainSourceStatic, StaticDomain: "bad/value"}, get: &countingNodeGetter{}, want: "invalid"},
		{name: "static label conflict", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, StaticDomain: "workers", NodeName: "worker-a"}, get: &countingNodeGetter{}, want: "conflicts"},
		{name: "missing node name", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel}, get: &countingNodeGetter{}, want: "NODE_NAME"},
		{name: "invalid label key", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, LabelKey: "bad/key/extra", NodeName: "worker-a"}, get: &countingNodeGetter{}, want: "label key"},
		{name: "api error", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{err: errors.New("unavailable")}, want: "get Kubernetes Node"},
		{name: "nil node", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{}, want: "nil"},
		{name: "wrong node", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-b"}}}, want: "returned"},
		{name: "absent label", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}}}, want: "missing"},
		{name: "empty label", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{TopologyKeyNetworkDomain: ""}}}}, want: "empty"},
		{name: "invalid label value", cfg: NodeDomainConfig{Source: NodeDomainSourceNodeLabel, NodeName: "worker-a"}, get: &countingNodeGetter{node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{TopologyKeyNetworkDomain: "bad/value"}}}}, want: "invalid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ResolveNodeDomain(t.Context(), test.cfg, test.get); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ResolveNodeDomain error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestCanonicalDomains(t *testing.T) {
	got, err := CanonicalDomains([]string{"fabric-b", "fabric-a", "fabric-b"})
	if err != nil || len(got) != 2 || got[0] != "fabric-a" || got[1] != "fabric-b" {
		t.Fatalf("CanonicalDomains = %v, %v; want [fabric-a fabric-b], nil", got, err)
	}
	for _, input := range [][]string{nil, {}, {""}, {"bad/value"}} {
		if _, err := CanonicalDomains(input); err == nil {
			t.Fatalf("CanonicalDomains(%v) = nil error", input)
		}
	}
}

func TestCanonicalReachableFromRequiresOwnerDomain(t *testing.T) {
	got, err := CanonicalReachableFrom("fabric-a", []string{"fabric-b", "fabric-a", "fabric-b"})
	if err != nil || len(got) != 2 || got[0] != "fabric-a" || got[1] != "fabric-b" {
		t.Fatalf("CanonicalReachableFrom = %v, %v", got, err)
	}
	if _, err := CanonicalReachableFrom("fabric-a", []string{"fabric-b"}); err == nil {
		t.Fatal("missing network domain membership accepted")
	}
}
