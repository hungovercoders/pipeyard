# Control plane image (used by `task deploy`; local dev runs uvicorn directly).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-group infra --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-group infra
RUN useradd --create-home pipeyard
USER pipeyard
# Bake the DuckDB httpfs extension into the image so nothing is downloaded at runtime.
RUN /app/.venv/bin/python -c "from pipeyard.query import install_extensions; install_extensions()"
EXPOSE 8000
CMD ["/app/.venv/bin/uvicorn", "--factory", "pipeyard.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
