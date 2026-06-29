# Dockerization + CI/CD for `auspex_lakehouse`

**Date:** 2026-06-28
**Status:** Implemented

## Goal

Run the `auspex_lakehouse` Dagster project in Docker (mirroring the
`dagster-quickstart` example's multi-service layout), inject the project's dlt
variables and secrets at runtime, and set up GitHub-based CI/CD.

## Context

`auspex_lakehouse` is a Dagster project (`dagster==1.13.11`) that uses:

- **dlt** (`dagster-dlt`) to ingest the NASA APOD / NeoWs APIs into an
  S3-compatible "bronze" lakehouse (`s3://auspex-lakehouse` @ `s3.datadazed.com`).
- **polars / deltalake / boto3** for a downstream `apod_images` asset that reads
  the Delta table and copies images into the bucket.
- **uv** for dependency management (`uv.lock` is the source of truth).

Two runtime secret/config surfaces exist:

1. **dlt** reads `dlt.secrets["nasa_api_key"]` and an S3 filesystem destination.
2. The `apod_images` asset (and `resources/delta.py`) read plain env vars
   (`MINIO_*`, `BRONZE_*`).

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Secret injection | **Env vars only**, via `env_file: .env` | No secrets baked into images; `.env` stays git-ignored; CI/CD supplies them as Actions secrets. |
| dlt config in containers | dlt **env-var convention** (`DESTINATION__FILESYSTEM__…`, `NASA_API_KEY`) | dlt natively reads these and they override `.dlt/*.toml`; no secret files in the image. |
| User-code image | **uv** + `uv.lock` (`--frozen`) | Reproducible, cached, carries the heavy pipeline deps. |
| Control-plane image | **uv**, but thin (pinned dagster packages only) | Webserver/daemon load user code over gRPC — they don't need pipeline deps. |
| Run execution | Dagster **DefaultRunLauncher** (runs execute in the user-code container) | Matches the example; env on `auspex_user_code` is sufficient — no per-run container env forwarding. |
| Postgres | `postgres:11` on a **named volume** | Faithful to the example; volume persists run history across restarts. |
| CI/CD | GitHub Actions: test → build → push to **GHCR** | Lint + tests + load-validation on PR; build & push tagged images on merge to `main`. |

## File inventory

**Created**

- `Dockerfile_user_code` — uv build of the gRPC code server.
- `Dockerfile_dagster` — uv build of the thin webserver/daemon image.
- `docker-compose.yaml` — `auspex_postgres` / `auspex_user_code` / `auspex_webserver` / `auspex_daemon`.
- `dagster.yaml` — Postgres storage (env-driven).
- `workspace.yaml` — gRPC server → `auspex_user_code:4000`.
- `.dockerignore` — excludes `.env`, `**/secrets.toml`, `.venv`, tmp/storage dirs.
- `.env.example` — committed template documenting every variable.
- `tests/test_definitions.py` — smoke test that the code location loads.
- `.github/workflows/ci.yml` — CI/CD pipeline.

**Modified**

- `.env` — added `NASA_API_KEY`, `DESTINATION__FILESYSTEM__*`, `DAGSTER_POSTGRES_*`.
- `.gitignore` — ignore `.dlt/secrets.toml` (both copies).
- `src/auspex_lakehouse/resources/delta.py` — read creds from env (was hard-coded).
- `pyproject.toml` — added `pytest` + `ruff` (dev) and a `[tool.ruff]` config.

## Secrets flow

```
.env  ──env_file──▶  auspex_user_code (+ auspex_daemon)
                       ├── dlt        reads NASA_API_KEY + DESTINATION__FILESYSTEM__*
                       └── apod_images asset reads MINIO_* / BRONZE_*
CI/CD: same variable names supplied from GitHub Actions secrets (deploy step).
```

`.dlt/config.toml` (non-secret: bucket URL, endpoint, region) stays committed;
`.dlt/secrets.toml` is git-ignored and `.dockerignore`-excluded.

## Running locally

```bash
cp .env.example .env      # fill in real values (already populated locally)
docker compose up --build
# Dagster UI → http://localhost:3000
```

## Security findings addressed

1. **`.dlt/secrets.toml` was not git-ignored** → added ignore rules. The repo is
   not yet git-initialized, so no history scrub is needed.
2. **Hard-coded AWS keys in `resources/delta.py`** → moved to env vars.
   **➜ Rotate that access key** — treat it as exposed.

## Live verification & gotchas fixed

The stack was built and run (`docker compose up --build`) and a real partition
was materialized end to end. Two non-obvious issues were found and fixed:

1. **`dagster-postgres` must be a project dependency.** With the DefaultRunLauncher,
   the run worker executes in the `user_code` container and reconstructs the
   instance (Postgres storage) from the launcher's instance ref — so that image
   must be able to `import dagster_postgres`. A clean `uv sync` from the lockfile
   didn't include it (it isn't a pipeline dep). The quickstart hides this with an
   explicit `pip install . dagster-postgres dagster-docker`. Fix: added
   `dagster-postgres` to `[project.dependencies]`. Symptom if missing:
   *"Couldn't import module dagster_postgres … DagsterPostgresStorage"*, run fails
   at dequeue with 0 steps.

2. **`.dlt/config.toml` may not hold credentials.** dlt treats
   `destination.filesystem.credentials` as a secret-typed value and refuses to read
   it from `config.toml`. The project had `endpoint_url` / `region_name` under
   `[destination.filesystem.credentials]` in `config.toml`. Fix: `config.toml` now
   holds only `bucket_url`; the credential fields live in `secrets.toml` (local) and
   come from `DESTINATION__FILESYSTEM__CREDENTIALS__*` env vars (container/CI).
   Symptom if wrong: *"Provider `config.toml` cannot hold secret values …
   ValueNotSecretException"* during the dlt `sync` step.

3. **Synology CPU lacks AVX2 → use `polars-lts-cpu`.** On the NAS, the user-code
   gRPC server died with return code **-4 (SIGILL, illegal instruction)** while
   importing the definitions: stock `polars` wheels require AVX2, which Synology
   Celeron/Atom CPUs don't have. Fix: depend on **`polars-lts-cpu`** (no-AVX2 build;
   ships wheels for all platforms incl. arm64), pinned `>=1.33,<1.34` since lts-cpu
   lags the main package. Symptom if wrong: container exits -4 on startup, no Python
   traceback from the asset code itself.

**Verified result:** partition `2026-06-28`, run `SUCCESS` —
`dlt_nasa_api_apod` rows_loaded=1, `dlt_nasa_api_neows` rows_loaded=2, written to
`dlt.destinations.filesystem` (s3://auspex-lakehouse), credentials supplied entirely
by container env vars.

## CI/CD improvements (recommended roadmap)

Implemented now:

- uv + lockfile builds, BuildKit cache mounts, and GHA layer caching.
- Lint (ruff) + tests + code-location load validation on every PR.
- Build on PR (no push); build & push to GHCR on merge to `main`, tagged
  `:latest` and `:<git-sha>` for traceable rollbacks.
- Secrets via env vars only; dummy values in CI so validation needs no real keys.

Next steps worth adding:

1. **Secret scanning** — `gitleaks`/`trufflehog` in CI + a pre-commit hook, so a
   future hard-coded key never lands in history.
2. **Image vulnerability scanning** — Trivy/Grype on the built images.
3. **Dependabot** — updates for Python deps, GitHub Actions, and Docker base images.
4. **Stronger load check** — `dagster definitions validate` / `dg check defs` in CI
   (in addition to the pytest smoke test).
5. **CD / deploy stage** — on tag or manual dispatch, SSH to the host and
   `docker compose pull && up -d`, or move to Dagster+/Kubernetes. This is where
   GitHub Actions *environment secrets* (NASA key, S3 creds) get consumed.
6. **Pull pre-built images in prod** — ✅ implemented as `docker-compose.prod.yaml`
   for the **Synology NAS** deployment target (x86_64, Container Manager / DSM 7.2+):
   `image: ${IMAGE_REPO}-{user-code,dagster}:${IMAGE_TAG}` from GHCR, Postgres
   bind-mounted to `/volume1/docker/auspex/pgdata`, `restart: unless-stopped`. The
   NAS builds nothing; it needs only the prod compose + `.env`. See the README
   "Deploy on a Synology NAS" section.
7. **Pin the base image by digest** and bump **Postgres off EOL 11** (e.g. 16).
8. **Consolidate the duplicate `.dlt/`** (repo root vs `src/auspex_lakehouse/`) to
   a single source of truth.
9. **Queue + concurrency** — add `QueuedRunCoordinator` to `dagster.yaml` once
   schedules/sensors drive real volume.
