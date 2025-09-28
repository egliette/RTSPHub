# Template Service Skeleton

This repository is a reusable template for new Python services. It includes a sensible project layout, Docker Compose scripts, CI workflows, and logging utilities.

## 1. 📦 Repository structure

- **app/**: Application code
  - **api/**: Routers/controllers (e.g., FastAPI routers and endpoints)
  - **core/**: App bootstrap, app factory, DI, security, constants
  - **config/**: App configuration
    - `settings.py`: Centralized runtime settings (env-driven)
  - **crud/**: Data-access helpers built on models/ORM
  - **database/**: DB engine/session creation and lifecycle
  - **models/**: ORM models (e.g., SQLAlchemy `Base` and model classes)
  - **schema/**: Pydantic request/response models
  - **services/**: Business/domain services and external integrations
  - **tests/**: Unit/integration tests
  - **utils/**: Shared utilities
    - `logger.py`: Structured logging setup and helpers
  - `main.py`: Application entrypoint (wire routers, middlewares, startup)
  - `__init__.py`: Package marker
- **scripts/**: Helper scripts for local dev, testing, and prod-like runs
  - `dev-compose.sh`: Start the development stack
  - `test-compose.sh`: Build/run tests in Docker
  - `prod-compose.sh`: Start a prod-like stack or attach a shell
- **.github/workflows/**: GitHub Actions CI/CD
  - `ci.yml`: Lint + build test image + run tests
  - `auto-bump-and-release.yml`, `publish-release.yml`: Release automation (optional)
- `docker-compose.yml`, `docker-compose.dev.yml`, `docker-compose.test.yml`: Compose definitions
- `.pre-commit-config.yaml`: Pre-commit hooks configuration
- `logs/` (created at runtime): Log files output directory

## 2. 🧩 Essential configuration to set for a new service

- **Service name**
  - Where: `app/utils/logger.py` and environment variables consumed by logging
  - Purpose: Identify your service in logs and metrics
  - How: Set env var `SERVICE_NAME` (and ensure `logger.py` reads it)

- **Docker network name**
  - Where: GitHub Actions `ci.yml` (env `DOCKER_NETWORK_NAME`) and local scripts
  - Purpose: Ensure containers communicate on a known network during CI and locally
  - How: In `.github/workflows/ci.yml`, set `DOCKER_NETWORK_NAME` under `env:`

- **Container name(s) for attach-shell flows**
  - Where: `scripts/dev-compose.sh`, `scripts/prod-compose.sh`
  - Purpose: Used by the scripts when running `docker exec -it <container> /bin/bash`
  - How: Set `CONTAINER_NAME` near the top of each script (dev: your dev app container; prod: your prod app container)

- **Compose network for local runs**
  - Where: `scripts/dev-compose.sh`, `scripts/prod-compose.sh`
  - Purpose: Ensure a consistent Docker network exists locally
  - How: Set `NETWORK_NAME` near the top of each script to match your compose files

- **Log directory and user mapping**
  - Where: `.env` or environment
  - Keys: `LOG_DIR`, `APP_UID`, `APP_GID`, `LOG_TO_STDOUT`, `LOG_LEVEL`, `LOG_MAX_DAYS`
  - Purpose: Make logs writable and correctly owned on host

## 3. 🚀 Quick start

1) Create a `.env` at repo root (optional but recommended):

```bash
SERVICE_NAME="your_service_name"
LOG_DIR="logs"
APP_UID="1000"
APP_GID="1000"
LOG_TO_STDOUT="true"
LOG_LEVEL="INFO"
LOG_MAX_DAYS="30"
```

2) Update script variables at the top of the relevant scripts:

```bash
# scripts/dev-compose.sh
NETWORK_NAME="your_local_network"
CONTAINER_NAME="your_dev_container_name"

# scripts/prod-compose.sh
NETWORK_NAME="your_local_network"
CONTAINER_NAME="your_prod_container_name"
```

3) Update CI network name in `ci.yml`:

```yaml
env:
  DOCKER_NETWORK_NAME: your_ci_network
```

## 4. 🛠️ Scripts usage

- `scripts/dev-compose.sh`
  - `--rebuild|-r`: Rebuild the app image before starting
  - `--shell|-s`: Start the app and attach an interactive shell inside the container

- `scripts/prod-compose.sh`
  - `--rebuild|-r`: Rebuild the app image before starting
  - `--shell|-s`: Start and attach an interactive shell to the prod container
  - `--workers|-w N`: Set the number of workers when running the app

- `scripts/test-compose.sh`
  - Builds and runs tests via `docker compose` using `docker-compose.test.yml`

## 5. ✅ CI overview

- Linting: `isort` and `black` run against `app/`
- Tests: Builds the test image, creates a CI network (`DOCKER_NETWORK_NAME`), runs tests, and cleans up

## 6. 📝 Notes

- Ensure `CONTAINER_NAME` matches the actual container name from your compose files when using `--shell`.
- Ensure `NETWORK_NAME` in scripts and `DOCKER_NETWORK_NAME` in CI are consistent with your compose definitions.
- The `logs/` directory is created on startup if missing; permissions are adjusted using `APP_UID`/`APP_GID`.

## 7. 🗃️ Alembic migrations (under `app/alembic`)

This template supports Alembic with migration scripts stored in `app/alembic`.

### 7.1 ⚙️ One-time setup

1) Install Alembic (in your environment or dev container):

```bash
pip install alembic
```

2) Initialize Alembic pointing to `app/alembic` (run from repo root):

```bash
alembic init app/alembic
```

This creates `app/alembic/` with `env.py` and a `versions/` directory, plus an `alembic.ini` in the repo root.

3) Edit `alembic.ini` to set the script location and DB URL source:

```ini
script_location = app/alembic
sqlalchemy.url = %(DATABASE_URL)s
```

Provide `DATABASE_URL` via environment when running Alembic.

4) Wire your models’ metadata in `app/alembic/env.py` so autogenerate works. Example:

```python
from app.models.base import Base  # adjust to your Base definition

target_metadata = Base.metadata
```

Ensure imports here do not require full app startup.

### 7.2 📜 Common commands (run from repo root)

- Create a new migration (autogenerate):

```bash
alembic revision --autogenerate -m "create users table"
```

- Apply latest migrations:

```bash
alembic upgrade head
```

- Downgrade one step:

```bash
alembic downgrade -1
```

### 7.3 🧭 Where to run

- Run Alembic commands from the repository root so `alembic.ini` is discovered.
- Migrations live under `app/alembic/versions/`.

## 8. 🔁 Pre-commit hooks

This template includes `.pre-commit-config.yaml` to enforce formatting and basic hygiene locally before commits.

### 8.1 ⚙️ One-time setup

```bash
pip install pre-commit
pre-commit install
```

This installs the git hook so checks run automatically on `git commit`.

### 8.2 ▶️ Run manually

Run on all files (good first run to normalize the repo):

```bash
pre-commit run --all-files
```

Run only changed files (what happens on commit):

```bash
pre-commit run
```

### 8.3 🧩 What runs

Configured hooks typically include:

- isort (imports ordering with Black profile)
- black (code formatting)
- end-of-file-fixer, trailing-whitespace, mixed-line-ending, etc.

See `.pre-commit-config.yaml` to adjust versions or add hooks.
