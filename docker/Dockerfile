FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY pyproject.toml README.md ./
RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY src ./src
COPY workspace.yaml ./
COPY .dagster ./.dagster

RUN poetry install --only main --no-root --no-interaction --no-ansi

CMD ["python", "-m", "poly_arbitrage.api.app"]
