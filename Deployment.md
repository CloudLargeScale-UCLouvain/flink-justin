# Deployment

:warning: We expect the reader followed the [instructions to install the required tools.](./Requirements.md)

### Creating a local cluster

By default, the scripts we use create a local cluster of 4 virtual nodes:
1. 1 control-plane
1. 1 manager (Prometheus, Grafana, ...)
1. 1 node hosting the JobManager
1. 1 node hosting the TaskManagers

 This value can be modified in the [cluster.yaml](./scripts/infra/kind/cluster.yaml) file by appending new more TaskManagers as follows:
 ```yaml
- role: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "tier=taskmanager"
 ```

 From the [Jupyter notebook web page](http://localhost:8888/notebooks/scripts/infra/kind/init-cluster.ipynb), open the [init-cluster.ipynb](./scripts/infra/kind/init-cluster.ipynb) file and execute the whole notebook.
 This will install the required python libraries, create the cluster, and install the services using Helm.

 :warning: If the command fails with a permission denied from Docker, although it works outside the jupyter notebook, execute the following command:
 ```bash
$ sudo chmod 666 /var/run/docker.sock
 ```

 ### Pushing the built images
To use the build Docker images, they need to be available from all nodes.
Instead of pushing the previously built images to a public repository, we can use the following commands to locally load them into the nodes created by Kind.

 ```bash
 $ kind load docker-image flink-justin:dais
 $ kind load docker-image flink-kubernetes-operator:dais
 ```

:warning: This operation can sometimes loop indefinitely although the operation succeeded.


### Deploying the Flink Kubernetes Operator
The Flink Kubernetes Operator is responsible of creating JobManagers, TaskManagers, and deploying Flink jobs on newly spawned workers. It also holds the logic of the autoscaler.

The operator is deployed automatically by the init-cluster notebook via `common_modules.sh`, pinned to the `jobmanager` node using a `nodeSelector`.

If you need to deploy it manually from the project root directory:
```bash
# From the root directory
$ helm install flink-kubernetes-operator ./flink-kubernetes-operator/helm/flink-kubernetes-operator \
  --set image.repository=flink-kubernetes-operator \
  --set image.tag=dais \
  --set operatorPod.nodeSelector.tier=jobmanager \
  -f ./flink-kubernetes-operator/examples/autoscaling/values.yaml
```
:warning: If you changed the name or the tag of the image when building it, please reflect the changes in the command.

To ensure that the operator is running:
```bash
$ kubectl get pods
NAME                                         READY   STATUS    RESTARTS   AGE
flink-kubernetes-operator-6569cb9b96-q4dbc   2/2     Running   0          3m25s
```

We are now all set to deploy a Flink job!
