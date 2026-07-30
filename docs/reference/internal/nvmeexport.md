# NVMeExport (Internal API)

!!! warning "Internal API"
    `NVMeExport` is an internal custom resource in a separate API group. The storage agent
    creates and manages `NVMeExport` objects to declare a desired NVMe-oF export; the `nvmet`
    controller reconciles them into the kernel `nvmet` configfs target. **Do not create,
    edit, or delete `NVMeExport` objects directly.** This reference exists for debugging and
    development.

`NVMeExport` declares a desired NVMe-oF subsystem, namespace, and access-control set that the
`nvmet` controller materialises in configfs. Its `spec` is written solely by the export's
creator (the storage agent) and treated as read-only by the controller; its `status` is
written solely by the controller.

| Property | Value |
| --- | --- |
| API group | `nvmet.randomvariable.co.uk` |
| API version | `v1alpha1` |
| Kind | `NVMeExport` |
| Short name | `nvex` |
| Scope | Namespaced |
| Finalizer | `nvmet.randomvariable.co.uk/export-protect` (deletion is blocked while an initiator holds a live connection) |

## Spec

Desired state, written by the storage agent.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `devicePath` | string | Yes | — | Absolute host path to the backing block device (for example `/dev/zvol/...`). Written verbatim to configfs; the controller does not interpret it. |
| `targetNQN` | string | Yes | — | NVMe subsystem NQN to own (deterministic per volume). |
| `portal` | string | Yes | — | `host:port` the initiator connects to (the `nvmet` port address). |
| `deviceGUID` | string | No | unset | Embedded in the namespace NGUID/EUI for stable identity. |
| `namespaceID` | integer | No | `1` | 1-based NVMe namespace ID. Minimum 1. |
| `allowedInitiators` | array of strings | No | empty | Desired allow-host set (initiator NQNs). The controller reconciles configfs `allowed_hosts` to exactly this set. |

## Status

Observed state, written by the `nvmet` controller.

| Field | Type | Description |
| --- | --- | --- |
| `conditions` | array | Standard condition list. Condition types: `Ready`, `Deleting`. |
| `state` | string | Lifecycle state: `Pending`, `Ready`, `Deleting`, or `Error`. |
| `admittedInitiators` | array of strings | Confirmed allow-host set present in configfs. The consumer polls this to confirm a publish. |
| `activeConnection` | boolean | Whether any initiator holds a live transport connection. Gates safe teardown on delete. |
| `observedGeneration` | integer | The `spec` generation last reconciled. |

## Printer Columns

`kubectl get nvex` shows: `NQN`, `Device`, `State`, `Active`, `Age`.

## See Also

- [Components and Workloads](../components.md) (reference)
- [Transport](../../explanation/transport.md) (explanation)
- [Volume (Internal API)](volume.md) (reference)

---

**Last Updated:** July 2026
**API Version:** nvmet.randomvariable.co.uk/v1alpha1
