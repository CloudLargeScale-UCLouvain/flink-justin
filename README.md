# Justin: Hybrid CPU/Memory Elastic Scaling for Distributed Stream Processing

> Artifact for the paper *"Justin: Hybrid CPU/Memory Elastic Scaling for Distributed Stream Processing"*,
> accepted at DAIS 2025.
> [arXiv:2505.19739](https://arxiv.org/abs/2505.19739) · [Zenodo](https://doi.org/10.5281/zenodo.15338861)

## What is Justin?

Justin is an auto-scaler for Apache Flink that combines **horizontal scaling** (adjusting operator parallelism) with **vertical memory scaling** (adjusting the amount of managed memory allocated to each operator). It extends [DS2](https://github.com/lsds/ds2), a state-of-the-art horizontal auto-scaler, with a memory dimension.

The core observation is that stateful stream processing operators (e.g., window aggregations, joins) rely on RocksDB, which uses a managed memory budget as a block cache. When the working set of a stateful operator grows beyond the cache size, RocksDB falls back to disk I/O, which increases state access latency and degrades throughput. In this regime, adding more parallel instances (horizontal scaling) does not help — each new instance gets the same small cache and faces the same I/O bottleneck. The right action is to give each instance more memory.

Justin detects this regime using two metrics per operator:
- **Cache hit rate** (Δθ): falls below 80% when the working set exceeds the cache.
- **State access latency** (Δτ): exceeds 1 ms (1,000,000 ns) when RocksDB is I/O-bound.

When both thresholds are violated for a stateful operator, Justin scales memory up (vertical). Otherwise, it delegates to DS2 for horizontal scaling. Memory is allocated in discrete levels that double from one level to the next, based on the default allocation. Stateless operators are assigned no managed memory (⊥).

### How the scaling policy works

The auto-scaler collects metrics over a full window (`job.autoscaler.metrics.window`) before evaluating any scaling decision. If no decision is taken at the end of a window, it waits for the next full window before re-evaluating. After a scaling action is applied, `job.autoscaler.stabilization.interval` prevents any further decision until the interval has elapsed, giving the cluster time to absorb the change.

At each decision point, Justin runs the following logic for each operator:

1. If the operator is **stateless**: assign ⊥ (no managed memory), delegate to DS2 for parallelism.
2. If the operator is **stateful** and either `cache_hit_rate < Δθ` or `state_latency > Δτ`: **scale memory up** (move to next memory level). Skip horizontal scaling for this operator in this interval.
3. Otherwise: **scale horizontally** using DS2's parallelism recommendation.

This hybrid policy ensures that memory-bound operators get more cache before adding parallelism, avoiding wasteful over-provisioning of CPU cores.

### Architecture

The operator exposes a custom REST endpoint (`/jobs/{job_id}/justin`) with two methods:
- `GET`: returns per-operator resource profiles, parallelism bounds, and RocksDB metrics, consumed by the auto-scaler and pushed to Prometheus via Pushgateway for observability.
- `PUT`: applies new resource requirements (memory level, parallelism) to a running job.

## Repository structure

```
dais/
├── flink-kubernetes-operator/   # Fork of the Flink K8s Operator with Justin's auto-scaler
│   └── flink-autoscaler/src/…/ScalingExecutor.java   # Justin policy (see policy() method)
├── flink/                       # Flink fork with custom REST endpoint + RocksDB metrics
├── notebooks/
│   ├── nexmark/                 # Macro benchmarks (Q1, Q2, Q3, Q5, Q8, Q11)
│   └── motivation/              # Motivation benchmarks (read-only, write-only, update)
├── scripts/
│   └── infra/
│       ├── common/              # Shared Helm values, ingress, dashboard configs
│       └── kind/                # Kind cluster setup (init-cluster.ipynb)
├── Requirements.md              # Tool installation + Docker image build
├── Deployment.md                # Cluster creation + operator deployment
└── Benchmarks.md                # Running experiments + accessing dashboards
```

## Getting started

Follow these steps in order:

1. **[Requirements.md](./Requirements.md)** — Install Jupyter, Kind, Helm, kubectl, and build the Docker images for Flink and the operator.
2. **[Deployment.md](./Deployment.md)** — Create the Kind cluster and deploy all services (Prometheus, Grafana, Pushgateway, Flink Kubernetes Operator).
3. **[Benchmarks.md](./Benchmarks.md)** — Run the Nexmark or motivation benchmarks and observe results in Grafana.

## Configuration reference

Justin's behavior is controlled via annotations in the `FlinkDeployment` YAML of each job:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `job.autoscaler.cache-hit-rate.min.threshold` | `0.8` | Cache hit rate below which memory scale-up is triggered (ratio, 0–1) |
| `job.autoscaler.state-latency.threshold` | `1000000.0` | State access latency above which memory scale-up is triggered (nanoseconds) |
| `job.autoscaler.stabilization.interval` | — | Cooldown after a scaling action; no new decision is taken until this interval has elapsed |
| `job.autoscaler.metrics.window` | — | Duration of the metrics collection window; a full window must elapse before each decision |

These parameters can be set per-job in the `flinkConfiguration` section of the `FlinkDeployment` spec. See [Benchmarks.md](./Benchmarks.md) for examples.