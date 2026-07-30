# Kubernetes Feature Policy

This project targets a future-Kubernetes testbed maintained by a Kubernetes-contributing organization.

- Kubernetes 1.36 is the minimum deployment version.
- Alpha and beta Kubernetes APIs and feature gates may be required and relied on when they support the project design.
- Do not reject or downgrade an implementation solely because a Kubernetes feature is alpha or beta. Review its API contract, security boundary, enablement requirements, upgrade behavior, and operational failure modes instead.
- PodCertificateRequest is an intentional dependency for node-bound NFS mTLS credentials. Keep its required feature gates documented and tested.

## Execution Discipline

- Build product, not courtroom evidence. Do not add identity, ownership, provenance, manifest-text, or implementation-detail verification unless it prevents a demonstrated failure or protects a real destructive/security boundary.
- Do not create tests that assert Dockerfile, Tekton YAML, Helm YAML, command text, or file contents contain expected strings. Validate behavior through the real build, deployment, or existing schema/tooling instead.
- Stop escalating harness and verification machinery when root cause has a direct product or configuration fix. Prefer smallest working change and run only checks capable of finding a credible regression.
- Never delay implementation for speculative edge-proofing, duplicate reviews, or evidence collection after enough information exists to act.
