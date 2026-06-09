#!/usr/bin/env bash
# Build the benchmark image and push it to the SPCS image repository.
#
# Env overrides:
#   SF_CONNECTION  Snowflake connection name (default: PM)
#   IMAGE_TAG      Image tag                (default: latest)
#   IMAGE_DB       Repo database            (default: IW_PLAYGROUND)
#   IMAGE_SCHEMA   Repo schema              (default: IW_TEST)
#   IMAGE_REPO     Image repository name    (default: IW_REPO)
#   IMAGE_NAME     Image name               (default: iwbench)
set -euo pipefail

SF_CONNECTION="${SF_CONNECTION:-PM}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_DB="${IMAGE_DB:-IW_PLAYGROUND}"
IMAGE_SCHEMA="${IMAGE_SCHEMA:-IW_TEST}"
IMAGE_REPO="${IMAGE_REPO:-IW_REPO}"
IMAGE_NAME="${IMAGE_NAME:-iwbench}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

cd "${repo_root}"

echo "[build] docker build (linux/amd64)"
docker build --platform linux/amd64 \
  -f spcs/Dockerfile \
  -t "${IMAGE_NAME}:${IMAGE_TAG}" \
  .

echo "[build] resolving registry URL via snow CLI"
# `snow spcs image-registry url` prints the account-level registry hostname.
REGISTRY_URL="$(snow spcs image-registry url --connection "${SF_CONNECTION}" 2>/dev/null | tail -n1)"
if [[ -z "${REGISTRY_URL}" ]]; then
  echo "[build] error: could not resolve registry URL (is snow CLI configured?)" >&2
  exit 1
fi
echo "[build] registry: ${REGISTRY_URL}"

# Snowflake image paths are case-insensitive but conventionally lowercase.
db_lc="$(echo "${IMAGE_DB}" | tr '[:upper:]' '[:lower:]')"
schema_lc="$(echo "${IMAGE_SCHEMA}" | tr '[:upper:]' '[:lower:]')"
repo_lc="$(echo "${IMAGE_REPO}" | tr '[:upper:]' '[:lower:]')"
TARGET="${REGISTRY_URL}/${db_lc}/${schema_lc}/${repo_lc}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "[build] login to registry"
snow spcs image-registry login --connection "${SF_CONNECTION}"

echo "[build] tag → ${TARGET}"
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${TARGET}"

echo "[build] push"
docker push "${TARGET}"

echo "[build] done: ${TARGET}"
