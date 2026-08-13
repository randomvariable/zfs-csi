# Observability Reference

zfs-csi exposes metrics, traces, and structured logs. This reference describes each signal
and how to configure it.

## Metrics

Each manager-backed component (the `controller`, `storage`, and `nvmet` modes) serves
Prometheus metrics over HTTP.

| Property | Value |
| --- | --- |
| Endpoint | `/metrics` |
| Default bind address | `:8080` |
| Flag | `--metrics-bind-address` |

The endpoint includes controller-runtime and Go client metric families plus
`zfs_csi_operations_duration_seconds{operation,status}`. The operation label is a
fixed source-code allowlist; resource IDs, datasets, targets, and initiators never
become Prometheus labels. `status` is `ok` or `error`.

**Note:** Prometheus `*Vec` metric families do not appear on `/metrics` until at least one
labelled series is recorded. A freshly started component may not list every family until it
has reconciled at least once.

## Tracing

The driver exports OpenTelemetry traces over the OTLP gRPC protocol.

| Property | Value |
| --- | --- |
| Protocol | OTLP over gRPC |
| Flag | `--tracing-otlp-endpoint` |
| Environment variable | `OTEL_EXPORTER_OTLP_ENDPOINT` |
| Service name | `zfs-csi` |

Tracing is disabled when no endpoint is configured. Set the flag or the environment variable
to a collector's `host:port` to enable it. The gRPC servers are instrumented, so CSI RPCs
and StagePlugin calls appear as spans.

## Logging

| Property | Value |
| --- | --- |
| Format | Structured JSON (zap encoder) |
| Timestamps | RFC 3339 |
| Destination | `stderr` |

The JSON encoder follows the Kubernetes SIG Instrumentation logging convention. Logs are
written to `stderr` so `stdout` stays a clean channel.

## The Health-Monitor Sidecar

The controller Deployment includes the CSI external health-monitor controller, which reports
volume health conditions. It serves its own HTTP endpoint on `:8081` within the controller
pod.

CSI 1.13 (KEP-1432) removed the `VOLUME_CONDITION` capability in favour of the
`ControllerGetVolumeHealth` / `NodeGetVolumeHealth` RPCs. The released sidecar (v0.18.0) still
requires the removed capability and exits against a v1.13 driver, so the chart pins the
staging build by digest until an upstream release ships the new RPCs.

## See Also

- [Command-Line Reference](command-line.md) (reference)
- [Components and Workloads](components.md) (reference)
- [Troubleshooting](../how-to/troubleshooting.md) (how-to)

---

**Last Updated:** July 2026
