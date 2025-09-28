FROM python:3.10-slim as base

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV VIRTUAL_ENV=/opt/venv

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && \
    apt-get install gcc g++ libpq-dev -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade setuptools wheel
RUN pip install --upgrade pip

ARG VERSION=0.0.0
ENV APP_VERSION=${VERSION}

COPY pyproject.toml .

RUN pip install --no-cache-dir ".[test]"

COPY app/ ./app/

RUN mkdir -p app && printf "%s" "${APP_VERSION}" > app/VERSION


ARG APP_UID=1000
ARG APP_GID=1000

RUN groupadd --gid ${APP_GID} app && \
    useradd --uid ${APP_UID} --gid ${APP_GID} --create-home --shell /bin/bash app

FROM base as production

RUN chown -R app:app /app
USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


FROM base as test

RUN chown -R app:app /app
USER app

CMD ["pytest", "-v", "app/tests"]


FROM base as development

RUN chown -R app:app /app
USER app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
