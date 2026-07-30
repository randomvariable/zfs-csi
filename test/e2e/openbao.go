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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

const (
	// openBaoNamespace / openBaoService give the in-cluster FQDN
	// openbao.openbao.svc:8200, which is the chart's encryption.openbao.addr
	// default — so the chart install needs no addr override.
	openBaoNamespace = "openbao"
	openBaoService   = "openbao"

	// openBaoImage is the dev-mode server image. Dev mode auto-initialises and
	// auto-unseals with an in-memory backend, which is exactly right for an
	// ephemeral, single-run E2E cluster (no persistence, no unseal ceremony).
	openBaoImage = "quay.io/openbao/openbao:2.2.0"

	// openBaoDevToken is the fixed dev-mode root token. The driver authenticates
	// to Transit with this via the chart's encryption.openbao.token. Safe only
	// because this OpenBao is ephemeral and reachable only inside the throwaway
	// E2E workload cluster.
	openBaoDevToken = "root"

	// openBaoTransitMount matches the chart's encryption.openbao.transitMount
	// default. The driver's Generate creates each per-volume Transit key on
	// demand under this mount, so the lane only needs the engine enabled.
	openBaoTransitMount = "transit"
)

// ensureOpenBaoInfra deploys a dev-mode OpenBao into the workload cluster and
// enables the Transit secrets engine, providing the KMS the driver uses for
// per-volume ZFS native encryption (DEK generation + crypto-shred). Idempotent:
// the Deployment/Service are applied (create-or-update) and the Transit enable
// tolerates an already-enabled mount.
//
// The driver's openbao Provider.Generate creates each per-volume Transit key on
// demand, so no per-key pre-creation is needed here — only the engine mount must
// exist. The chart is installed separately (installDriverFromChart) with
// encryption enabled + the dev token; its encryption.openbao.addr/transitMount
// defaults already resolve to this deployment.
func ensureOpenBaoInfra(ctx context.Context, kubeconfig string) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %[1]s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[2]s
  namespace: %[1]s
  labels:
    app: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[2]s
  template:
    metadata:
      labels:
        app: %[2]s
    spec:
      containers:
        - name: openbao
          image: %[3]s
          args:
            - server
            - -dev
            - -dev-root-token-id=%[4]s
            - -dev-listen-address=0.0.0.0:8200
          ports:
            - name: api
              containerPort: 8200
          readinessProbe:
            httpGet:
              path: /v1/sys/health?standbyok=true
              port: 8200
            initialDelaySeconds: 3
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  selector:
    app: %[2]s
  ports:
    - name: api
      port: 8200
      targetPort: 8200
`, openBaoNamespace, openBaoService, openBaoImage, openBaoDevToken)

	apply := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(manifest)
	if out, err := apply.CombinedOutput(); err != nil {
		return fmt.Errorf("apply openbao manifest: %w\n%s", err, string(out))
	}

	if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"-n", openBaoNamespace, "rollout", "status", "deployment/"+openBaoService, "--timeout", "3m",
	).CombinedOutput(); err != nil {
		return fmt.Errorf("wait openbao rollout: %w\n%s", err, string(out))
	}

	// Enable the Transit engine. In dev mode the server listens on localhost with
	// the root token; enabling is idempotent-tolerant here because a re-run hits
	// an already-mounted path ("path is already in use"), which we treat as
	// success after confirming the mount is present.
	enable := fmt.Sprintf(
		"BAO_ADDR=http://127.0.0.1:8200 BAO_TOKEN=%s bao secrets enable transit 2>&1 || true",
		openBaoDevToken,
	)
	if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"-n", openBaoNamespace, "exec", "deploy/"+openBaoService, "--", "sh", "-c", enable,
	).CombinedOutput(); err != nil {
		return fmt.Errorf("enable transit engine: %w\n%s", err, string(out))
	}

	// Verify the engine is mounted so a silent enable failure fails the lane loudly
	// rather than surfacing later as an opaque driver Generate error.
	verify := fmt.Sprintf(
		"BAO_ADDR=http://127.0.0.1:8200 BAO_TOKEN=%s bao secrets list",
		openBaoDevToken,
	)
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"-n", openBaoNamespace, "exec", "deploy/"+openBaoService, "--", "sh", "-c", verify,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("list secrets engines: %w\n%s", err, string(out))
	}
	if !strings.Contains(string(out), openBaoTransitMount+"/") {
		return fmt.Errorf("transit engine not mounted after enable:\n%s", string(out))
	}

	return nil
}
