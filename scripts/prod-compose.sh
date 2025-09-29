#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR%/scripts}"

COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"
NETWORK_NAME=""
CONTAINER_NAME=""
APP_SERVICE=""

REBUILD=false
ATTACH_SHELL=false
SCALE_WORKERS=4

usage() {
  echo "Usage: $(basename "$0") [--rebuild|-r] [--shell|-s] [--workers|-w NUM] [--help|-h]" >&2
  echo "  --rebuild, -r     Rebuild app image before starting" >&2
  echo "  --shell, -s       Attach to app container shell instead of running the app" >&2
  echo "  --workers, -w     Number of worker processes (default: 4)" >&2
  echo "  --help, -h        Show this help message" >&2
  echo "" >&2
  echo "Examples:" >&2
  echo "  $(basename "$0")                    # Start prod stack normally" >&2
  echo "  $(basename "$0") --shell            # Start stack and attach to app shell" >&2
  echo "  $(basename "$0") --rebuild --shell  # Rebuild app and attach to shell" >&2
  echo "  $(basename "$0") --workers 8        # Start with 8 worker processes" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild|-r)
      REBUILD=true
      shift
      ;;
    --shell|-s)
      ATTACH_SHELL=true
      shift
      ;;
    --workers|-w)
      SCALE_WORKERS="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

echo "Bringing down existing prod stack..."
docker-compose -f "$COMPOSE_FILE" down -v || true

echo "Ensuring network '$NETWORK_NAME' exists..."
docker network create "$NETWORK_NAME" >/dev/null 2>&1 || true

# Load environment variables
if [[ -f "${REPO_ROOT}/.env" ]]; then
  echo "Loading environment variables from .env..."
  set -a
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Warning: .env file not found. Using default values."
  LOG_DIR="logs"
  APP_UID="1000"
  APP_GID="1000"
fi

# Create logs directory
echo "Creating logs directory: ${LOG_DIR:-logs}"
mkdir -p "${REPO_ROOT}/${LOG_DIR:-logs}"

echo "Setting ownership of logs directory to app user (UID: ${APP_UID:-1000}, GID: ${APP_GID:-1000})..."
if chown "${APP_UID:-1000}:${APP_GID:-1000}" "${REPO_ROOT}/${LOG_DIR:-logs}" 2>/dev/null; then
  echo "✓ Successfully changed ownership of logs directory"
else
  echo "⚠ Failed to change ownership of logs directory"
  echo "  This might be because you need to run with sudo or the UID/GID don't match your user"
  echo "  You can run this command manually:"
  echo "  sudo chown ${APP_UID:-1000}:${APP_GID:-1000} ${REPO_ROOT}/${LOG_DIR:-logs}"
  echo "  Or run the script with sudo: sudo $0"
fi

if [[ "$REBUILD" == "true" ]]; then
  echo "Determining build version from latest tag..."
  VERSION="$(git -C "$REPO_ROOT" describe --tags --abbrev=0 2>/dev/null || true)"
  VERSION="${VERSION#v}"
  if [[ -z "${VERSION}" ]]; then VERSION="0.0.0"; fi

  echo "Rebuilding app image with VERSION=${VERSION}..."
  docker-compose -f "$COMPOSE_FILE" build --build-arg VERSION="${VERSION}" "${APP_SERVICE}"
fi

if [[ "$ATTACH_SHELL" == "true" ]]; then
  echo "Starting app container and attaching to shell..."
  # Ensure app is running and stays alive, then attach a shell
  docker-compose -f "$COMPOSE_FILE" up -d "${APP_SERVICE}"
  docker exec -it "${CONTAINER_NAME}" /bin/bash
else
  echo "Starting prod stack with app (${SCALE_WORKERS} workers)..."
  # Update the command to use the specified number of workers
  export WORKERS="${SCALE_WORKERS}"
  docker-compose -f "$COMPOSE_FILE" run --rm -e WORKERS="${SCALE_WORKERS}" "${APP_SERVICE}" uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers "${SCALE_WORKERS}"
fi

echo "Done."
