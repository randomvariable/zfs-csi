# Vendored CNI for the E2E workload cluster

`calico.yaml` is Calico v3.32.1 from
https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/calico.yaml
applied via the CAPI framework CNIManifestPath. calico-node tolerates all
NoSchedule taints so it reaches the tainted storage node.
