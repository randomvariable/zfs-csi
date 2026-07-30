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

package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/randomvariable/zfs-csi/internal/tlsca"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsureNFSTLS verifies signer-issued server material before a
// TLS NFS volume becomes usable. It deliberately uses only direct reads: a
// storage agent must never load or create the CA signing key or leaf secrets.
func EnsureNFSTLS(ctx context.Context, reader client.Reader, namespace, owner, endpoint string) error {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return fmt.Errorf("NFS TLS requires a configured NFS endpoint")
	}

	ca := &corev1.Secret{}
	if err := reader.Get(ctx, apimachinerytypes.NamespacedName{Namespace: namespace, Name: tlsca.CAPublicSecretName}, ca); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("NFS TLS public CA is not ready")
		}
		return fmt.Errorf("get NFS TLS public CA: %w", err)
	}
	leafName, err := tlsca.ServerSecretName(owner)
	if err != nil {
		return fmt.Errorf("NFS TLS server leaf owner: %w", err)
	}
	leaf := &corev1.Secret{}
	if err := reader.Get(ctx, apimachinerytypes.NamespacedName{Namespace: namespace, Name: leafName}, leaf); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("NFS TLS server leaf is not ready")
		}
		return fmt.Errorf("get NFS TLS server leaf: %w", err)
	}
	if !tlsca.ServerLeafValid(leaf.Data["tls.crt"], leaf.Data["tls.key"], ca.Data["ca.crt"], endpoint, 0) {
		return fmt.Errorf("NFS TLS server leaf is invalid for endpoint %q", endpoint)
	}

	return nil
}
