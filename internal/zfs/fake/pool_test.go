// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package fake

import (
	"context"
	"testing"
)

func TestPoolIdentityAndHealth(t *testing.T) {
	a := New().WithPool("tank", 10)
	b := New().WithPool("tank", 10)
	ga, err := a.PoolGUID(context.Background(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	gb, err := b.PoolGUID(context.Background(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	if ga == gb {
		t.Fatalf("same raw pool name got same GUID %q across fake nodes", ga)
	}
	if health, err := a.PoolHealth(context.Background(), "tank"); err != nil || health != "ONLINE" {
		t.Fatalf("health=%q err=%v", health, err)
	}
	a.ReplacePool("tank", 10, "123", "DEGRADED")
	if guid, _ := a.PoolGUID(context.Background(), "tank"); guid != "123" {
		t.Fatalf("replacement GUID=%q", guid)
	}
}
