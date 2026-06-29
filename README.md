# auspex_lakehouse

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

### Running with Docker

The project ships a multi-service Docker deployment (Dagster webserver + daemon +
user-code gRPC server + Postgres), mirroring the official `dagster-quickstart`
layout. See [docs/superpowers/specs/2026-06-28-docker-cicd-design.md](docs/superpowers/specs/2026-06-28-docker-cicd-design.md)
for the design.

1. Provide secrets/config (already git-ignored):

   ```bash
   cp .env.example .env    # then fill in real values
   ```

   `.env` is loaded by `dagster dev` locally and injected into the containers via
   `env_file:`. dlt reads `NASA_API_KEY` and `DESTINATION__FILESYSTEM__*` from these
   env vars; the `apod_images` asset reads `MINIO_*` / `BRONZE_*`. No secrets are
   baked into the images.

2. Build and start the stack:

   ```bash
   docker compose up --build -d
   ```

3. Open the UI at http://localhost:3000. Check status / logs / tear down with:

   ```bash
   docker compose ps
   docker compose logs -f auspex_webserver
   docker compose down          # add -v to also drop the Postgres volume
   ```

Images are built from `Dockerfile_user_code` (your code + pipeline deps, via uv)
and `Dockerfile_dagster` (thin control plane). CI builds and pushes both to GHCR
on merge to `main` (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

### Deploy on a Synology NAS (Container Manager)

The NAS **pulls pre-built images from GHCR — it never builds** (the user-code
image is large and compiles native wheels). Use
[docker-compose.prod.yaml](docker-compose.prod.yaml), which uses `image:` instead
of `build:`.

**One-time:** push to GitHub so CI publishes the images, then either make the two
GHCR packages (`…-user-code`, `…-dagster`) **public** (repo → Packages → package
settings), or keep them private and add a `ghcr.io` login with a `read:packages`
PAT in **Container Manager → Registry → Settings**.

1. In **File Station**, create `/volume1/docker/auspex` and
   `/volume1/docker/auspex/pgdata`.
2. Put two files in `/volume1/docker/auspex`:
   - `docker-compose.prod.yaml`, renamed to **`docker-compose.yml`**
   - **`.env`** — copy from `.env.example`, fill in the real secrets, and set
     `IMAGE_REPO=ghcr.io/<owner>/<repo>` (lowercase) + `IMAGE_TAG=latest`.
     `.env` is git-ignored, so copy it over manually (e.g. via File Station).
3. **Container Manager → Project → Create** → name `auspex`, path
   `/volume1/docker/auspex`; it detects the compose file → **Next** → it pulls the
   images and starts the stack.
4. Open **http://10.0.0.24:3000**. (If DSM's firewall is enabled, allow TCP 3000.)

**Update:** push to `main` (CI republishes `:latest`), then in Container Manager
open the project → **Action → Build** to re-pull and recreate. To pin or roll
back, set `IMAGE_TAG` to a specific `:<git-sha>` in `.env` and rebuild.

> The NAS needs only those two files — no source checkout. Postgres data lives in
> `/volume1/docker/auspex/pgdata`, so it survives restarts and is covered by DSM
> backups.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
