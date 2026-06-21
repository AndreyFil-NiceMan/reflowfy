# Reflowfy base image

The three Reflowfy service images (`api`, `reflow-manager`, `worker`) all build
`FROM` a shared **base image** that bakes in `reflowfy` + all of its
dependencies. This means dependencies are installed **once** (in the base),
instead of three times across the service builds.

- The service Dockerfiles only add the **user's** project dependencies
  (`requirements.txt` and/or `pyproject.toml`/uv) and copy the pipeline code.
- The base image is **published by the maintainer** on each release and
  distributed as a `tar.gz` attached to the GitHub Release.

## For end users — consume the base image

A GitHub Release is not a container registry, so the base image is shipped as a
loadable tar rather than something `docker pull` can fetch. One-time per version:

```bash
# 1. Download reflowfy-base-<version>.tar.gz from the GitHub Release, then:
docker load < reflowfy-base-<version>.tar.gz
# This loads local images: reflowfy-base:<version> and reflowfy-base:latest

# 2. Point your project at it (already the default in .env):
#    REFLOWFY_BASE_IMAGE=reflowfy-base:latest   (or reflowfy-base:<version>)

# 3. Build your service images as usual — they build FROM the loaded base:
reflowfy run --build      # local
reflowfy build            # build + push service images for deploy
```

`REFLOWFY_BASE_IMAGE` (in `.env`) controls which base image the service builds
use. It is consumed by `docker-compose.yml`, `reflowfy build`, and the E2E suite.

## For maintainers — publish the base image

Publishing is automated by `.github/workflows/release.yml`. When you **publish a
GitHub Release** whose tag matches `reflowfy.__version__` (e.g. tag `v1.0.12`
for version `1.0.12`), the workflow:

1. builds the wheel + sdist and uploads them to **PyPI** (Trusted Publishing / OIDC),
2. waits for PyPI to index the new version,
3. builds the base image from the published package,
4. `docker save`s it to `reflowfy-base-<version>.tar.gz`,
5. attaches that tar to the same Release.

### Release checklist

1. Bump `__version__` in `reflowfy/__init__.py`.
2. Commit and push.
3. Create a GitHub Release with tag `v<version>` (must match `__version__`) and
   publish it. The workflow does the rest.

### One-time PyPI setup (Trusted Publishing)

On PyPI, add a **trusted publisher** for the project pointing at this repo and
the `release.yml` workflow. No PyPI token/secret is stored in the repo.

### Build the base image manually

```bash
# Build + push to a registry (reads REFLOWFY_BASE_IMAGE / REFLOWFY_VERSION / PYTHON_IMAGE from env or .env)
./scripts/build_base_image.sh

# Build locally only (no push), e.g. to produce a tar yourself:
REFLOWFY_VERSION=1.0.12 ./scripts/build_base_image.sh --no-push
docker save reflowfy-base:1.0.12 | gzip > reflowfy-base-1.0.12.tar.gz
```

## E2E tests

E2E tests run against **unreleased** code, so they cannot use the published base.
`scripts/run_e2e_tests.sh` builds a local `reflowfy-base:local` from the freshly
built wheel and points the service + mock builds at it via
`REFLOWFY_BASE_IMAGE=reflowfy-base:local`.
