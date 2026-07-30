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

package openbao

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/randomvariable/zfs-csi/internal/crypto"
)

func TestProviderGenerateFetchDelete(t *testing.T) {
	fixture := newProviderFixture(t)
	defer fixture.server.Close()

	provider, err := New(fixture.server.URL+"/", "token", "/transit/", fixture.server.Client())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("generates reference", func(t *testing.T) {
		ref, err := provider.Generate(context.Background(), "vol-1")
		if err != nil {
			t.Fatalf("Generate() error = %v", err)
		}

		if !strings.HasPrefix(ref, "transit/zfs-vol-vol-1/") {
			t.Fatalf("ref = %q", ref)
		}

		fixture.ref = ref
	})

	t.Run("fetches generated key", func(t *testing.T) {
		fetched, err := provider.Fetch(context.Background(), fixture.ref)
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}

		if !bytes.Equal(fetched, fixture.raw) {
			t.Fatal("Fetch() raw mismatch")
		}
	})

	t.Run("deletes transit key", func(t *testing.T) {
		if err := provider.Delete(context.Background(), fixture.ref); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
	})

	t.Run("records expected request count", func(t *testing.T) {
		// createKey, datakey, decrypt, enable-deletion (config), delete = 5.
		if len(fixture.requests) != 5 {
			t.Fatalf("requests = %d, want 5", len(fixture.requests))
		}
	})
}

func TestProviderKubernetesAuthUsesLoginTokenForTransit(t *testing.T) {
	fixture := newProviderFixture(t)
	fixture.wantTransitToken = "k8s-logged-in"
	defer fixture.server.Close()

	provider, err := New(fixture.server.URL+"/", "static-token", "/transit/", fixture.server.Client(), WithKubernetesAuth("zfs-csi", "k8s-token-xyz"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := provider.Generate(context.Background(), "vol-1"); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	if len(fixture.requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(fixture.requests))
	}

	if got, want := fixture.requests[0].path, "/v1/auth/kubernetes/login"; got != want {
		t.Fatalf("first request path = %q, want %q", got, want)
	}
}

type providerRequest struct {
	method string
	path   string
}

type providerFixture struct {
	server           *httptest.Server
	requests         []providerRequest
	ciphertext       string
	raw              []byte
	ref              string
	wantTransitToken string
}

func newProviderFixture(t *testing.T) *providerFixture {
	t.Helper()

	fixture := &providerFixture{
		raw:              []byte("0123456789abcdef0123456789abcdef"),
		ciphertext:       "vault:v1:testcipher",
		wantTransitToken: "token",
	}

	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.handle(t, w, r)
	}))

	return fixture
}

func (f *providerFixture) handle(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	f.requests = append(f.requests, providerRequest{method: r.Method, path: r.URL.Path})

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/auth/kubernetes/login":
		f.handleKubernetesLogin(t, w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transit/keys/zfs-vol-vol-1":
		f.assertTransitToken(t, r)
		f.handleCreateKey(t, w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transit/datakey/plaintext/zfs-vol-vol-1":
		f.assertTransitToken(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
			"ciphertext": f.ciphertext,
			"plaintext":  base64.StdEncoding.EncodeToString(f.raw),
		}})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transit/decrypt/zfs-vol-vol-1":
		f.assertTransitToken(t, r)
		f.handleDecrypt(t, w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transit/keys/zfs-vol-vol-1/config":
		// Delete() enables deletion on the key before removing it.
		f.assertTransitToken(t, r)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && r.URL.Path == "/v1/transit/keys/zfs-vol-vol-1":
		f.assertTransitToken(t, r)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *providerFixture) assertTransitToken(t *testing.T, r *http.Request) {
	t.Helper()

	if got := r.Header.Get("X-Vault-Token"); got != f.wantTransitToken {
		t.Fatalf("token header = %q, want %q", got, f.wantTransitToken)
	}
}

func (f *providerFixture) handleKubernetesLogin(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	if got := r.Header.Get("X-Vault-Token"); got != "" {
		t.Fatalf("login token header = %q, want empty", got)
	}

	var body map[string]string
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body["role"] != "zfs-csi" || body["jwt"] != "k8s-token-xyz" {
		t.Fatalf("login body = %#v", body)
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]string{"client_token": "k8s-logged-in"}})
}

func (f *providerFixture) handleCreateKey(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	var body map[string]any

	_ = json.NewDecoder(r.Body).Decode(&body)
	exportable, ok := body["exportable"].(bool)
	if body["type"] != keyType || !ok || !exportable {
		t.Fatalf("create key body = %#v", body)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (f *providerFixture) handleDecrypt(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	var body dataKeyResponse

	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Ciphertext != f.ciphertext {
		t.Fatalf("ciphertext = %q, want %q", body.Ciphertext, f.ciphertext)
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"plaintext": base64.StdEncoding.EncodeToString(f.raw)}})
}

func TestProviderNotFoundMapsToCryptoError(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	provider, err := New(server.URL, "", "transit", server.Client())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ref := encodeRef("missing", "vault:v1:nope")

	_, err = provider.Fetch(context.Background(), ref)
	if !errors.Is(err, crypto.ErrKeyNotFound) {
		t.Fatalf("Fetch() error = %v, want ErrKeyNotFound", err)
	}
}

// TestProviderDeleteIsIdempotent asserts a crypto-shred of an already-gone key
// succeeds: OpenBao Transit returns HTTP 400 "no existing key ... could be
// found" for the keys/<name>/config (enable-deletion) call when the key was
// already deleted on a prior reconcile pass. Delete must treat that as success
// so the agent's reconcileDelete finalizer completes instead of looping. Seen
// live in AWS conformance (delete retry after a successful first shred).
func TestProviderDeleteIsIdempotent(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"http 400 no existing key", http.StatusBadRequest, `{"errors":["no existing key named zfs-vol-x could be found"]}`},
		{"http 404 not found", http.StatusNotFound, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			defer server.Close()

			provider, err := New(server.URL, "root", "transit", server.Client())
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := provider.Delete(context.Background(), encodeRef("zfs-vol-x", "vault:v1:cipher")); err != nil {
				t.Fatalf("Delete() of already-gone key = %v, want nil (idempotent shred)", err)
			}
		})
	}
}

func TestProviderParsesOpenBaoErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	}))
	defer server.Close()

	provider, err := New(server.URL, "", "transit", server.Client())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = provider.Fetch(context.Background(), encodeRef("key", "vault:v1:cipher"))
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("Fetch() error = %v, want parsed permission denied", err)
	}
}
