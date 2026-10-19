#!/bin/bash
# Deploy (or update) the E2E sink in the current OpenShift/Kubernetes namespace.
#   IMAGE=<any image with python>  sink/deploy.sh
# Then reach it from the test runner with:  oc port-forward svc/e2e-sink 8090:8080
set -euo pipefail
cd "$(dirname "$0")"
: "${IMAGE:?set IMAGE to an image with python, e.g. the reflowfy worker image}"
CLI=${CLI:-oc}
$CLI create configmap e2e-sink-code --from-file=server.py -o yaml --dry-run=client | $CLI apply -f - >/dev/null
sed "s|__IMAGE__|$IMAGE|" k8s.yaml | $CLI apply -f - >/dev/null
# the code is a mounted file: restart so an updated server.py is picked up
$CLI rollout restart deploy/e2e-sink >/dev/null
$CLI rollout status deploy/e2e-sink --timeout=180s
