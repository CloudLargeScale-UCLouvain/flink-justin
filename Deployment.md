# Deployment

> **Prerequisites:** Follow [Requirements.md](./Requirements.md) first to install all tools and build the Docker images.

## Cluster layout

The Kind cluster consists of 4 virtual nodes:

| Node | Role | Label |
|------|------|-------|
| control-plane | Kubernetes control plane | — |
| worker (manager) | Prometheus, Grafana, Pushgateway | `tier=manager`, `ingress-ready=true` |
| worker (jobmanager) | Flink JobManager, Operator | `tier=jobmanager` |
| worker (taskmanager) | Flink TaskManagers | `tier=taskmanager` |

The manager node has ports 80 and 443 mapped to the host, which is required for the ingress controller to be reachable from outside the cluster. ingress-nginx is pinned to this node via the `ingress-ready=true` label.

Additional TaskManager nodes can be added by appending workers to [scripts/infra/kind/cluster.yaml](./scripts/infra/kind/cluster.yaml):

```yaml
- role: worker
  kubeadmConfigPatches:
    - |
      kind: JoinConfiguration
      nodeRegistration:
        kubeletExtraArgs:
          node-labels: "tier=taskmanager"
```

## Setting INGRESS_IP

By default, all ingress hostnames resolve to `127.0.0.1` (local access). If you are deploying on a remote VM and want to reach the services from your machine, export the VM's IP before running the cluster init notebook:

```bash
$ export INGRESS_IP=<your-vm-ip>
```

IPv6 addresses are supported — colons are automatically replaced with dashes to comply with the `sslip.io` hostname format (e.g. `2001:db8::1` → `2001-db8--1`).

## Creating the cluster

From the [Jupyter notebook web page](http://localhost:8888), open [scripts/infra/kind/init-cluster.ipynb](./scripts/infra/kind/init-cluster.ipynb) and run all cells. This will:

1. Create the Kind cluster.
2. Load the Docker images onto the relevant nodes (targeting only the nodes that need each image).
3. Install all services via Helm (Prometheus, Grafana, Pushgateway, cert-manager, local-path-provisioner).
4. Deploy the ingress controller pinned to the manager node.
5. Deploy the Flink Kubernetes Operator pinned to the jobmanager node.

> **If the notebook fails with a Docker permission error**, run:
> ```bash
> $ sudo chmod 666 /var/run/docker.sock
> ```

## Deploying the Flink Kubernetes Operator manually

The operator is deployed automatically by the init notebook. If you need to redeploy it manually, run the following from the **repository root**:

```bash
$ helm install flink-kubernetes-operator ./flink-kubernetes-operator/helm/flink-kubernetes-operator \
  --set image.repository=flink-kubernetes-operator \
  --set image.tag=dais \
  --set operatorPod.nodeSelector.tier=jobmanager \
  -f ./flink-kubernetes-operator/examples/autoscaling/values.yaml
```

> **Note:** If you changed the image name or tag during the build step, update `--set image.repository` and `--set image.tag` accordingly.

To verify the operator is running:

```bash
$ kubectl get pods
NAME                                         READY   STATUS    RESTARTS   AGE
flink-kubernetes-operator-6569cb9b96-q4dbc   2/2     Running   0          3m25s
```

The operator is now ready to deploy Flink clusters and jobs. See [Benchmarks.md](./Benchmarks.md) for next steps.