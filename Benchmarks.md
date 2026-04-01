# Running the benchmarks

## Accessing the dashboards

The deployment scripts install ingresses for the three services. Hostnames are based on the `INGRESS_IP` environment variable (defaults to `127.0.0.1` for local use; set to the VM's IP when running remotely).

- Prometheus: `prometheus.<INGRESS_IP>.sslip.io` ([prometheus.127.0.0.1.sslip.io](http://prometheus.127.0.0.1.sslip.io) by default)
- Grafana: `grafana.<INGRESS_IP>.sslip.io` ([grafana.127.0.0.1.sslip.io](http://grafana.127.0.0.1.sslip.io) by default) — login: `admin` / `prom-operator`
- Flink UI: `flink.<INGRESS_IP>.sslip.io` ([flink.127.0.0.1.sslip.io](http://flink.127.0.0.1.sslip.io) by default)

## Nexmark benchmarks

The Nexmark benchmark suite evaluates streaming query performance over a synthetic auction event stream. Open [./notebooks/nexmark/xp.ipynb](http://localhost:8888/notebooks/notebooks/nexmark/xp.ipynb) in your browser.

### Queries

Each query exercises a different workload pattern:

| Query | Pattern | Stateful? | What to observe |
|-------|---------|-----------|-----------------|
| Q1 | Map (currency conversion) | No | Horizontal scaling only; memory stays at ⊥ |
| Q2 | Filter (item selection) | No | Horizontal scaling only; memory stays at ⊥ |
| Q3 | Join + filters (person/auction) | Yes | Memory scale-up when working set grows |
| Q5 | Sliding-window aggregation (hot items) | Yes | Memory scale-up under high cardinality |
| Q8 | Tumbling-window join (new sellers) | Yes | Vertical then horizontal scaling |
| Q11 | Session-window aggregation (user bids) | Yes | Memory scale-up with session state growth |

### Running an experiment

Before running, make sure the Flink image name in each query YAML matches what you built:

```yaml
# notebooks/nexmark/qX/queryX.yaml
spec:
  image: flink-justin:dais   # ← must match your built image tag
```

Open the notebook and run all cells. Each query cell:
1. Submits the query to the Flink cluster.
2. Waits until a Flink job reaches `RUNNING` state.
3. Runs the experiment for a fixed duration, pushing managed memory metrics to Prometheus every 30 seconds.
4. The experiment runs **twice**: once with the default DS2 auto-scaler, once with Justin.

Watch the **Flink – Justin** Grafana dashboard to observe:
- Source throughput staying stable as the auto-scaler reacts to load.
- Operator parallelism changes (horizontal scaling decisions by DS2).
- Managed memory per operator jumping between levels (158 MB → 316 MB → 632 MB) when Justin detects cache pressure.
- For stateless operators (Q1, Q2): memory remains at 0 — only parallelism changes.

> **Note on dashboard plots vs. the paper:** The Grafana plots can differ from those shown in the paper in absolute numbers (the paper used a Grid5000 cluster with dedicated hardware). The key patterns to look for are the same: memory step-ups for stateful operators under Justin, and parallelism-only changes under the DS2 baseline. The number of scaling decisions and their timing may also differ due to resource contention on a local environment using Kind.

### Tuning Justin's parameters

Justin's two main thresholds can be adjusted per query in the YAML file:

```yaml
flinkConfiguration:
  job.autoscaler.cache-hit-rate.min.threshold: "0.8"   # trigger memory scale-up below this cache hit rate
  job.autoscaler.state-latency.threshold: "1000000.0"  # trigger memory scale-up above this latency (ns)
```

Lowering the cache hit rate threshold (e.g., `0.7`) makes Justin more tolerant before scaling memory. Lowering the latency threshold (e.g., `500000.0`) makes it more aggressive. You can also adjust:

- `job.autoscaler.stabilization.interval`: cooldown between scaling decisions.
- `job.autoscaler.metrics.window`: averaging window for metrics.

## Motivation benchmarks

Open [./notebooks/motivation/xp.ipynb](http://localhost:8888/notebooks/notebooks/motivation/xp.ipynb) in your browser.

These benchmarks isolate the three RocksDB access patterns to motivate the need for vertical scaling:

- **read-only** ([query.yaml](./notebooks/motivation/read-only/query.yaml)): only reads from state.
- **write-only** ([query.yaml](./notebooks/motivation/write-only/query.yaml)): only writes to state.
- **update** ([query.yaml](./notebooks/motivation/update/query.yaml)): mixed reads and writes.

Make sure the image name matches your build before running.

## Running a custom query

You can run any Flink job under Justin's auto-scaler by writing a `FlinkDeployment` manifest. Below is a minimal template:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: my-job
spec:
  image: flink-justin:dais           # your Flink image
  flinkVersion: v1_17
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "1"
    state.backend: "rocksdb"
    state.backend.rocksdb.memory.managed: "true"
    # Justin parameters
    job.autoscaler.cache-hit-rate.min.threshold: "0.8"
    job.autoscaler.state-latency.threshold: "1000000.0"
    job.autoscaler.stabilization.interval: "1m"
    job.autoscaler.metrics.window: "3m"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "1024m"
      cpu: 1
    podTemplate:
      spec:
        nodeSelector:
          tier: jobmanager
  taskManager:
    resource:
      memory: "1024m"
      cpu: 1
    podTemplate:
      spec:
        nodeSelector:
          tier: taskmanager
  job:
    jarURI: local:///opt/flink/usrlib/my-job.jar
    parallelism: 1
    upgradeMode: stateless
```

### Steps

1. **Build your JAR** with `mvn package` (or equivalent) and copy it into your Flink Docker image under `/opt/flink/usrlib/`.

2. **Load the image** into Kind:
   ```python
   # In notebooks/infra/kind/init-cluster.ipynb or a new cell
   kind_load("flink-justin:dais")
   ```

3. **Submit the job**:
   ```bash
   kubectl apply -f my-job.yaml
   ```

4. **Monitor** via the Flink UI (`flink.<INGRESS_IP>.sslip.io`) and Grafana (`grafana.<INGRESS_IP>.sslip.io`).

5. **Modify the Justin policy** (optional): The scaling decision logic is in [`ScalingExecutor.java`](./flink-kubernetes-operator/flink-autoscaler/src/main/java/org/apache/flink/autoscaler/ScalingExecutor.java) — see the `policy()` method. After modifying, rebuild the operator image and reload it:
   ```bash
   # From repo root
   docker build -t flink-kubernetes-operator:dais ./flink-kubernetes-operator
   kind load docker-image flink-kubernetes-operator:dais
   # Delete and redeploy the operator
   bash scripts/delete.sh
   helm install flink-kubernetes-operator ./flink-kubernetes-operator/helm/flink-kubernetes-operator \
     --set image.repository=flink-kubernetes-operator \
     --set image.tag=dais \
     --set operatorPod.nodeSelector.tier=jobmanager \
     -f ./flink-kubernetes-operator/examples/autoscaling/values.yaml
   ```
