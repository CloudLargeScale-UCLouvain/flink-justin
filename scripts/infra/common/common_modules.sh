#!/bin/bash

export PATH=$HOME/tools:$PATH # for grid5k

# give full rights to pod running in default (bad practice, but ok in our experimentation case)
kubectl apply -f ./cluster-role-binding-default.yaml

# ingress (works in 1.21 max)
#kubectl apply -f ../common/nginx-controller.yaml

kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.5.3/cert-manager.yaml
kubectl --namespace cert-manager rollout status deployment/cert-manager-webhook

# storage
kubectl create namespace manager
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.21/deploy/local-path-storage.yaml
kubectl apply -f ./cm-local-path.yaml # override default directory to /tmp (caution on reboot !)

sleep 30 # sleep for 30 seconds for application of configuration in local-path

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install --namespace manager --version 30.0.2 prom prometheus-community/kube-prometheus-stack -f ./values-prom.yaml
helm install --namespace manager pushgateway prometheus-community/prometheus-pushgateway \
  --set nodeSelector.tier=manager \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.namespace=manager \
  --set serviceMonitor.additionalLabels.release=prom

kubectl apply -f pod-monitor.yaml

helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install loki grafana/loki-stack --namespace manager --version 2.6.0 -f ./values-loki.yaml

sleep 10 # sometimes timeout

# repo for kowl
helm repo add cloudhut https://raw.githubusercontent.com/cloudhut/charts/master/archives
helm repo update

# grafana dashboard
kubectl apply -f ./grafana-dashboard-flink.yaml

kubectl apply -f ../common/new_ingress.yaml

# flink kubernetes operator
helm install flink-kubernetes-operator ../../../flink-kubernetes-operator/helm/flink-kubernetes-operator \
  --set image.repository=flink-kubernetes-operator \
  --set image.tag=dais \
  --set operatorPod.nodeSelector.tier=jobmanager \
  -f ../../../flink-kubernetes-operator/examples/autoscaling/values.yaml