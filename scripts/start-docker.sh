#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

cleanup_on_failure() {
  status=$?
  if (( status != 0 )); then
    docker compose logs || true
    docker compose down --volumes --remove-orphans || true
  fi
  exit "$status"
}
trap cleanup_on_failure EXIT

# Reset only this fixture project's containers and data volume.
docker compose down --volumes --remove-orphans
docker compose --profile setup pull
docker compose up --detach minio

ready=false
for _ in {1..60}; do
  if curl --connect-timeout 1 --max-time 2 --fail --silent --show-error \
    http://127.0.0.1:9000/minio/health/ready >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "MinIO did not become ready" >&2
  exit 1
fi

# A failed or hung bucket initialization must fail the test setup.
timeout 60s docker compose run --rm --no-deps --interactive=false -T mc
