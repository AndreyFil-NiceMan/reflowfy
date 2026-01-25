# Reflowfy: Fresh Project Setup Guide

This guide walks you through creating a new project that uses Reflowfy, from zero to deployed on OpenShift.

## 1. Install Reflowfy

### Option A: Install from Git (Recommended)
```bash
pip install git+https://github.com/YOUR_USERNAME/reflowfy.git
```

### Option B: Install from a Wheel
Build the wheel first (in the source project):
```bash
cd /path/to/reflowfy-source
pip install build
python -m build
```
Then install the wheel in your new project:
```bash
pip install /path/to/reflowfy-source/dist/reflowfy-0.1.0-py3-none-any.whl
```

---

## 2. Initialize Your Project

Create a new project directory and initialize it:
```bash
mkdir my-data-project
cd my-data-project
reflowfy init --name my_first_pipeline
```

This creates:
```
my-data-project/
├── pipelines/
│   ├── __init__.py
│   └── my_first_pipeline.py   # Your pipeline
├── Dockerfile.api
├── Dockerfile.reflow-manager
├── Dockerfile.worker
└── docker-compose.yml
```

---

## 3. Customize Your Pipeline

Edit `pipelines/my_first_pipeline.py`:
```python
from reflowfy import Pipeline, ElasticsearchSource, KafkaDestination

@Pipeline.register("my_first_pipeline")
def my_first_pipeline():
    return Pipeline(
        name="my_first_pipeline",
        source=ElasticsearchSource(
            host="http://elasticsearch:9200",
            index="my-index",
            query={"match_all": {}}
        ),
        destination=KafkaDestination(topic="output-topic"),
        transformations=[
            # Add your data transformations
        ]
    )
```

---

## 4. Test Locally

Run everything locally with Docker Compose:
```bash
reflowfy run --build
```
Access:
- API: http://localhost:8000
- Manager: http://localhost:8001/docs

Trigger a test:
```bash
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "my_first_pipeline"}'
```

---

## 5. Build for OpenShift

Build and push images to your private registry:
```bash
reflowfy build --registry registry.lab.local --project my-project
```

---

## 6. Deploy to OpenShift

```bash
# Login to your cluster
oc login https://your-openshift-cluster

# Deploy
reflowfy deploy \
  --registry registry.lab.local \
  --kafka kafka.lab.local:9092
```

Get access URLs:
```bash
oc get routes
```

---

## 7. Verify

Check pod status:
```bash
reflowfy check
```

---

## CLI Command Reference

| Command | Description |
|---------|-------------|
| `reflowfy init` | Scaffold a new project |
| `reflowfy run` | Run locally with Docker Compose |
| `reflowfy build` | Build & push images to registry |
| `reflowfy deploy` | Deploy to OpenShift with Helm |
| `reflowfy check` | Verify deployment health |
