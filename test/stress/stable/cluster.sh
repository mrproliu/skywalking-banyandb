#!/bin/bash

set -ex
tmp_dir=$1
banyandb_repo=$2
banyandb_tag=$3
banyandb_values_file=$4
banyandb_namespace=$5
prometheus_values_file=$6
grafana_values_file=$7

if [ -z "$tmp_dir" ] || [ -z "$banyandb_repo" ] || [ -z "$banyandb_tag" ] || [ -z "$banyandb_values_file" ] || [ -z "$banyandb_namespace" ] || [ -z "$prometheus_values_file" ] || [ -z "$grafana_values_file" ]; then
  echo "Usage: $0 <tmp_dir> <banyandb_repo> <banyandb_tag> <banyandb_values_file> <banyandb_namespace> <prometheus_values_file> <grafana_values_file>"
  exit 1
fi

cd $tmp_dir
# installing banyandb
if [ ! -d "skywalking-banyandb-helm/.git" ]; then
  git clone https://github.com/apache/skywalking-banyandb-helm
fi
cd skywalking-banyandb-helm
helm dependency build chart
helm upgrade -i banyandb chart -f $banyandb_values_file \
  --set image.repository=$banyandb_repo --set image.tag=$banyandb_tag \
  -n $banyandb_namespace --create-namespace
cd ..

# installing prometheus
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
kubectl create ns monitoring --dry-run=client -o yaml | kubectl apply -f -
helm upgrade -i prom prometheus-community/prometheus \
  -n monitoring -f $prometheus_values_file

# install grafana
helm upgrade -i gf grafana/grafana \
  -n monitoring -f $grafana_values_file

# patching grafana dashboard
curl -o grafana-dashboard.json https://raw.githubusercontent.com/apache/skywalking-banyandb/refs/heads/main/docs/operation/grafana-cluster.json
dashboard_path=$(pwd)/grafana-dashboard-patched.json
export DS_PROMETHEUS=prometheus
envsubst '${DS_PROMETHEUS}' < "grafana-dashboard.json" > "$dashboard_path"

cat <<EOF | kubectl -n monitoring apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: banyandb-grafana-dashboard
  labels:
    grafana_dashboard: "1"
data:
  banyandb.json: |
$(sed 's/^/    /' "$dashboard_path")
EOF

sleep 3
kubectl -n monitoring get cm -l grafana_dashboard -o wide
