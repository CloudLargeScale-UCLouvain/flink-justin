# Requirements

## Minimum system requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 8 cores | 16 cores |
| Memory | 16 GB RAM | 32 GB RAM |
| Storage | 30 GB free | 50 GB free |

The Flink codebase is large and the Maven build produces several GB of artifacts. The Flink images are duplicated on each TaskManager node, increasing the minimum amount of available storage as the number of TaskManager nodes grows. The Kind cluster runs 4 virtual nodes (control-plane, manager, jobmanager, taskmanager), each hosting multiple containers (Prometheus, Grafana, Flink operator, TaskManagers), which adds up quickly in memory.

## Notebooks

We assume that the user has `python3` installed. The Jupyter server **must be started from the root directory of the project** so that notebook-relative paths (e.g. `scripts/infra/kind/`) resolve correctly.

1. Create a virtual environment from the root directory:
```bash
$ python3 -m venv venv
$ source venv/bin/activate
```
2. Install and launch Jupyter:
```bash
$ pip install notebook
$ python3 -m notebook
```
The last command will print a URL (e.g. `http://localhost:8888/?token=…`) — open it in your browser to access the notebooks.

## Cluster tools

> **Note:** Kind and Helm both require Docker to be installed.

Install Docker:
```bash
$ curl -fsSL https://get.docker.com -o get-docker.sh
$ sudo sh get-docker.sh
# Add your user to the 'docker' group (the group is created by the installer)
$ sudo usermod -aG docker $USER
$ newgrp docker
```

Install the remaining tools:

1. Kind → [Installation](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
2. Helm → [Installation](https://helm.sh/docs/helm/helm_install/)
3. kubectl → [Installation](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/#install-kubectl-binary-with-curl-on-linux)

Install `envsubst` (used to template ingress hostnames — part of the `gettext` package, may not be present by default):
```bash
# Debian/Ubuntu
$ sudo apt-get install -y gettext-base
# RHEL/Fedora
$ sudo dnf install -y gettext
```

## Compiling Justin

To compile the Flink code base along with the benchmarks, we provide a Dockerfile located at the root folder. It has 3 stages:

1. **`build`** — uses the official Maven Docker image to compile Flink with fine-grain memory allocation enabled through Justin. The Flink uber jar and libraries are in `/app/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/` inside the image.
2. **`benchmark`** — compiles the motivation and Nexmark benchmarks.
3. **`main`** — uses the official Flink Docker image, replaces the default executables with those from the `build` stage, and copies the benchmarks into the new image.

Build the image from the root directory:
```bash
# From the root directory
$ docker build . -t flink-justin:dais
```

> **Warning:** The Flink code base is very large. This build can take **up to 15 minutes** and requires a stable internet connection for Maven dependency downloads.

## Compiling the Flink Kubernetes Operator

From the `flink-kubernetes-operator` directory:
```bash
# From the flink-kubernetes-operator directory
$ docker build . -t flink-kubernetes-operator:dais
```

This build takes about 3 minutes.
