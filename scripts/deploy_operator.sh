./tools/helm install flink-kubernetes-operator ../flink-kubernetes-operator/helm/flink-kubernetes-operator/ --set image.repository=$DOCKER_ID/flink-kubernetes-operator --set image.tag=dais -f ../flink-kubernetes-operator/examples/autoscaling/values.yaml