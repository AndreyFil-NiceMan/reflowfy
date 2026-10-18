#!/bin/bash
# Copy the suite's pipelines into a Reflowfy project so the next image build includes them.
#   tests/e2e_distributed/sync_pipelines.sh /path/to/project
set -euo pipefail
dest="${1:?usage: sync_pipelines.sh <project dir>}/src/pipelines"
[ -d "$dest" ] || { echo "no $dest — is that a reflowfy project (reflowfy init)?" >&2; exit 1; }
cp "$(dirname "$0")"/pipelines/dist_pipelines.py "$dest/"
echo "copied dist_pipelines.py -> $dest (rebuild the api/manager/worker images and redeploy)"
