# OpenShift / Red Hat Kubernetes Deployment Guide

This guide details how to deploy Reflowfy on a private Red Hat OpenShift cluster.

## 1. Prerequisites
- **Admin Access**: You need `oc` CLI access to your cluster.
- **Image Registry**: A container registry accessible by your OpenShift cluster (e.g., the internal OpenShift registry, Harbor, or Nexus).

## 2. Prepare Images (Non-Root)
We have updated the Dockerfiles to run as a non-root user (`reflowfy`, UID 1001). This is critical for OpenShift security compliance.

### Build and Push
Run these commands on your machine where you have the code:

```bash
# Set your registry URL
export REGISTRY="<your-private-registry-url>"
export PROJECT="reflowfy"

# 1. Build Images
docker build -f Dockerfile.api -t $REGISTRY/$PROJECT/api:latest .
docker build -f Dockerfile.reflow-manager -t $REGISTRY/$PROJECT/reflow-manager:latest .
docker build -f Dockerfile.worker -t $REGISTRY/$PROJECT/worker:latest .

# 2. Login to Registry
docker login $REGISTRY

# 3. Push Images
docker push $REGISTRY/$PROJECT/api:latest
docker push $REGISTRY/$PROJECT/reflow-manager:latest
docker push $REGISTRY/$PROJECT/worker:latest
```

## 3. Configure Helm for OpenShift

Create a `openshift-values.yaml` file to override defaults for your environment:

```yaml
# openshift-values.yaml

global:
  imagePullSecrets:
    - name: <your-registry-secret> # If authentication is required

# API Configuration
api:
  image:
    repository: <your-private-registry-url>/reflowfy/api
    tag: "latest"
  service:
    type: ClusterIP # OpenShift uses Routes, not NodePort usually

# ReflowManager Configuration
reflowManager:
  image:
    repository: <your-private-registry-url>/reflowfy/reflow-manager
    tag: "latest"
  service:
    type: ClusterIP

# Worker Configuration
worker:
  image:
    repository: <your-private-registry-url>/reflowfy/worker
    tag: "latest"

# Security Context (OpenShift handles User IDs automatically)
podSecurityContext:
  fsGroup: null
  runAsUser: null
  runAsGroup: null
  runAsNonRoot: true # We verify image is non-root compatible

# External Kafka (if using your specific lab Kafka)
kafka:
  enabled: false
  external:
    bootstrapServers: "<your-kafka-host>:9092"
```

## 4. Deploy

```bash
# Create project (namespace)
oc new-project reflowfy

# Allow random UID if strictly needed (usually needed for Bitnami/Postgres persistence)
oc adm policy add-scc-to-user anyuid -z default

# Install
helm install reflowfy ./helm/reflowfy -f openshift-values.yaml
```

## 5. Expose Services via Routes
OpenShift uses `Routes` instead of `NodePort` or `LoadBalancer`.

```bash
# Expose Manager (UI)
oc expose svc/reflowfy-reflow-manager

# Expose API
oc expose svc/reflowfy-api
```

Get the public URLs:
```bash
oc get routes
```
