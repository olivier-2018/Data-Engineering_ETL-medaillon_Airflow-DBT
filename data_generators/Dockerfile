FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml ./
RUN uv sync --no-dev

COPY . .

# config.yaml is mounted read-only by docker-compose (not baked in), so edits
# don't require a rebuild.
ENTRYPOINT ["uv", "run", "python", "main.py"]
