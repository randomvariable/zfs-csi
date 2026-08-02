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

// Package openbao implements a crypto.KeyProvider backed by the OpenBao
// Transit secrets engine.
package openbao

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/randomvariable/zfs-csi/internal/crypto"
)

const (
	keyPrefix   = "zfs-vol-"
	dekSize     = 32
	refPrefix   = "transit"
	keyType     = "aes256-gcm96"
	maxErrBody  = 4096
	httpTimeout = 10 * time.Second
)

var (
	errAddrRequired             = errors.New("openbao: addr is required")
	errTransitMountRequired     = errors.New("openbao: transit mount is required")
	errInvalidRawDEKSize        = errors.New("openbao: invalid raw dek size")
	errMissingDatakeyCiphertext = errors.New("openbao: datakey response missing ciphertext")
	errMissingDatakeyPlaintext  = errors.New("openbao: datakey response missing plaintext")
	errMissingDecryptPlaintext  = errors.New("openbao: decrypt response missing plaintext")
	errMissingAuthClientToken   = errors.New("openbao: kubernetes auth response missing client token")
	errOpenBaoHTTPStatus        = errors.New("openbao: unexpected http status")
	errMalformedKeyReference    = errors.New("openbao: malformed key reference")
)

type dataKeyResponse struct {
	Ciphertext string `json:"ciphertext"`
	Plaintext  string `json:"plaintext"`
}

type kubernetesAuth struct {
	role    string
	jwtPath string
}

// Option configures an OpenBao provider.
type Option func(*Provider)

// WithKubernetesAuth authenticates to OpenBao via the Kubernetes auth method.
func WithKubernetesAuth(role, jwt string) Option {
	return func(p *Provider) {
		p.kubernetesAuth = &kubernetesAuth{role: role, jwtPath: jwt}
	}
}

// Provider is an OpenBao Transit-backed per-volume DEK provider.
type Provider struct {
	addr         string
	token        string
	transitMount string
	httpClient   *http.Client

	kubernetesAuth     *kubernetesAuth
	authenticatedToken string
	authMu             sync.Mutex
}

// New returns an OpenBao Transit KeyProvider.
func New(addr, token, transitMount string, httpClient *http.Client, opts ...Option) (*Provider, error) {
	addr = strings.TrimRight(addr, "/")
	transitMount = strings.Trim(transitMount, "/")

	if addr == "" {
		return nil, errAddrRequired
	}

	if transitMount == "" {
		return nil, errTransitMountRequired
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: httpTimeout}
	}

	provider := &Provider{addr: addr, token: token, transitMount: transitMount, httpClient: httpClient}
	for _, opt := range opts {
		opt(provider)
	}

	return provider, nil
}

// Generate creates a Transit key, asks OpenBao for a plaintext data key, and
// returns a reference containing both the Transit key name and ciphertext.
func (p *Provider) Generate(ctx context.Context, volumeID string) (string, error) {
	keyName := keyPrefix + volumeID
	if err := p.createTransitKey(ctx, keyName); err != nil {
		return "", err
	}

	raw, ciphertext, err := p.dataKey(ctx, keyName)
	if err != nil {
		return "", err
	}

	if len(raw) != dekSize {
		return "", fmt.Errorf("%w: generated raw dek must be %d bytes, got %d", errInvalidRawDEKSize, dekSize, len(raw))
	}

	return encodeRef(keyName, ciphertext), nil
}

// Fetch decrypts the ciphertext embedded in ref and returns the raw 32-byte DEK.
func (p *Provider) Fetch(ctx context.Context, ref string) ([]byte, error) {
	keyName, ciphertext, err := parseRef(ref)
	if err != nil {
		return nil, err
	}

	raw, err := p.decrypt(ctx, keyName, ciphertext)
	if err != nil {
		return nil, err
	}

	if len(raw) != dekSize {
		return nil, fmt.Errorf("%w: raw dek must be %d bytes, got %d", errInvalidRawDEKSize, dekSize, len(raw))
	}

	return raw, nil
}

// Delete destroys the Transit key for ref, crypto-shredding the DEK. It is
// idempotent: if the key is already gone (a retried delete after a successful
// first shred), it returns nil rather than erroring, so the agent's
// reconcileDelete can complete the finalizer instead of looping forever.
func (p *Provider) Delete(ctx context.Context, ref string) error {
	keyName, _, err := parseRef(ref)
	if err != nil {
		return err
	}

	// OpenBao Transit refuses to delete a key unless deletion is explicitly
	// enabled on it first (400 "deletion is not allowed for this key"). Set
	// deletion_allowed=true via the key config, then delete (crypto-shred).
	// A missing key here means it was already shredded on a prior pass — treat
	// that as success (the DEK is gone, which is the whole goal).
	if err := p.do(ctx, http.MethodPost, p.mountPath("keys", keyName, "config"), map[string]any{
		"deletion_allowed": true,
	}, nil); err != nil {
		if isKeyMissing(err) {
			return nil
		}
		return err
	}

	if err := p.do(ctx, http.MethodDelete, p.mountPath("keys", keyName), nil, nil); err != nil {
		if isKeyMissing(err) {
			return nil
		}
		return err
	}
	return nil
}

// isKeyMissing reports whether err indicates the Transit key does not exist.
// OpenBao returns this as either an HTTP 404 (crypto.ErrKeyNotFound) or, for the
// keys/<name>/config and delete endpoints, an HTTP 400 whose body contains "no
// existing key ... could be found". Both mean the key is already gone, so a
// crypto-shred that hits either has already achieved its goal.
func isKeyMissing(err error) bool {
	if errors.Is(err, crypto.ErrKeyNotFound) {
		return true
	}
	return strings.Contains(err.Error(), "no existing key") &&
		strings.Contains(err.Error(), "could be found")
}

func (p *Provider) createTransitKey(ctx context.Context, keyName string) error {
	return p.do(ctx, http.MethodPost, p.mountPath("keys", keyName), map[string]any{
		"type":       keyType,
		"exportable": true,
	}, nil)
}

func (p *Provider) dataKey(ctx context.Context, keyName string) ([]byte, string, error) {
	var out transitResponse
	if err := p.do(ctx, http.MethodPost, p.mountPath("datakey", "plaintext", keyName), nil, &out); err != nil {
		return nil, "", err
	}

	if out.Data.Ciphertext == "" {
		return nil, "", errMissingDatakeyCiphertext
	}

	if out.Data.Plaintext == "" {
		return nil, "", errMissingDatakeyPlaintext
	}

	raw, err := base64.StdEncoding.DecodeString(out.Data.Plaintext)
	if err != nil {
		return nil, "", fmt.Errorf("openbao: decode datakey plaintext: %w", err)
	}

	return raw, out.Data.Ciphertext, nil
}

func (p *Provider) decrypt(ctx context.Context, keyName, ciphertext string) ([]byte, error) {
	var out transitResponse
	if err := p.do(ctx, http.MethodPost, p.mountPath("decrypt", keyName), map[string]any{
		"ciphertext": ciphertext,
	}, &out); err != nil {
		return nil, err
	}

	if out.Data.Plaintext == "" {
		return nil, errMissingDecryptPlaintext
	}

	raw, err := base64.StdEncoding.DecodeString(out.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("openbao: decode plaintext: %w", err)
	}

	return raw, nil
}

type transitResponse struct {
	Data dataKeyResponse `json:"data"`
}

func (p *Provider) do(ctx context.Context, method, path string, in any, out any) error {
	token, err := p.authToken(ctx)
	if err != nil {
		return err
	}

	err = p.doWithToken(ctx, method, path, in, out, token)
	if err == nil || p.kubernetesAuth == nil || !isAuthRejected(err) {
		return err
	}

	// Do not clear a token installed by another request while this request was
	// in flight.
	p.authMu.Lock()
	if p.authenticatedToken == token {
		p.authenticatedToken = ""
	}
	p.authMu.Unlock()

	token, err = p.authToken(ctx)
	if err != nil {
		return err
	}
	return p.doWithToken(ctx, method, path, in, out, token)
}

func (p *Provider) authToken(ctx context.Context) (string, error) {
	if p.kubernetesAuth == nil {
		return p.token, nil
	}

	p.authMu.Lock()
	defer p.authMu.Unlock()

	if p.authenticatedToken != "" {
		return p.authenticatedToken, nil
	}

	jwt, err := os.ReadFile(p.kubernetesAuth.jwtPath)
	if err != nil {
		return "", fmt.Errorf("openbao: read Kubernetes JWT: %w", err)
	}

	var out struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err := p.doWithToken(ctx, http.MethodPost, "auth/kubernetes/login", map[string]string{
		"role": p.kubernetesAuth.role,
		"jwt":  strings.TrimSpace(string(jwt)),
	}, &out, ""); err != nil {
		return "", err
	}

	if out.Auth.ClientToken == "" {
		return "", errMissingAuthClientToken
	}

	p.authenticatedToken = out.Auth.ClientToken
	return p.authenticatedToken, nil
}

type httpStatusError struct {
	status int
	err    error
}

func (e *httpStatusError) Error() string { return e.err.Error() }
func (e *httpStatusError) Unwrap() error { return e.err }

func isAuthRejected(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) &&
		(statusErr.status == http.StatusUnauthorized || statusErr.status == http.StatusForbidden)
}

func (p *Provider) doWithToken(ctx context.Context, method, path string, in any, out any, token string) error {
	var body io.Reader

	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("openbao: marshal request: %w", err)
		}

		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, p.addr+"/v1/"+path, body)
	if err != nil {
		return fmt.Errorf("openbao: build request: %w", err)
	}

	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("openbao: request failed: %w", err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNotFound {
		return crypto.ErrKeyNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseError(resp)
	}

	if out == nil {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("openbao: decode response: %w", err)
	}

	return nil
}

func parseError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))

	var parsed struct {
		Errors []string `json:"errors"`
		Error  string   `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		if len(parsed.Errors) > 0 {
			return fmt.Errorf("%w: %s: %s", errOpenBaoHTTPStatus, resp.Status, strings.Join(parsed.Errors, "; "))
		}

		if parsed.Error != "" {
			return fmt.Errorf("%w: %s: %s", errOpenBaoHTTPStatus, resp.Status, parsed.Error)
		}
	}

	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = resp.Status
	}

	return &httpStatusError{
		status: resp.StatusCode,
		err:    fmt.Errorf("%w: %s: %s", errOpenBaoHTTPStatus, resp.Status, msg),
	}
}

func (p *Provider) mountPath(parts ...string) string {
	return p.transitMount + "/" + strings.Join(parts, "/")
}

func encodeRef(keyName, ciphertext string) string {
	// RawURLEncoding (alphabet A-Za-z0-9-_ , no padding) so the ref matches the
	// Volume CRD pattern ^(transit|kv)/[a-zA-Z0-9._/-]{1,255}$ and never emits a
	// '/' (which would break parseRef's split). StdEncoding's '+' and '=' both
	// violate the pattern, and its '/' breaks the split.
	return refPrefix + "/" + keyName + "/" + base64.RawURLEncoding.EncodeToString([]byte(ciphertext))
}

func parseRef(ref string) (keyName string, ciphertext string, err error) {
	parts := strings.Split(ref, "/")
	if len(parts) != 3 || parts[0] != refPrefix || parts[1] == "" || parts[2] == "" {
		return "", "", errMalformedKeyReference
	}

	ciphertextBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", fmt.Errorf("openbao: malformed ciphertext reference: %w", err)
	}

	return parts[1], string(ciphertextBytes), nil
}

var _ crypto.KeyProvider = (*Provider)(nil)
