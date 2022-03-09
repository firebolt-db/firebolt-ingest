FROM python:3.9-slim as base

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY ./src ./src
COPY ./setup.cfg .
COPY ./pyproject.toml .

FROM base as dev

RUN pip install -e ".[dev]"
COPY ./tests ./tests
CMD ["python3", "/app/src/firebolt_ingest/main.py"]

FROM base as prod

RUN pip install .

CMD ["firebolt_ingest"]
