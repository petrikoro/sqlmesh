#!/usr/bin/env bash
# Installs the Docker CLI and Compose plugin into a non-Docker base image.
# Used by GitLab CI engine-test jobs that need docker-in-docker.
set -euo pipefail

DOCKER_VERSION="${DOCKER_VERSION:-27.5.1}"
COMPOSE_VERSION="${COMPOSE_VERSION:-2.32.4}"
ARCH="$(uname -m)"

apt-get update && apt-get install -y --no-install-recommends ca-certificates curl

curl -fsSL "https://download.docker.com/linux/static/stable/${ARCH}/docker-${DOCKER_VERSION}.tgz" \
  | tar xz -C /usr/local/bin --strip-components=1 docker/docker

mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
  "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-linux-${ARCH}"
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo "Installed Docker CLI ${DOCKER_VERSION} and Compose ${COMPOSE_VERSION}"
