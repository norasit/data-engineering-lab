FROM python:3.12-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core==1.12.4" \
    "dbt-bigquery==1.12.0" \
    && pip check

# Project, profile and credentials are supplied only through runtime mounts.
ENV DBT_PROFILES_DIR=/usr/app/dbt/profiles \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /usr/app/dbt/citibike
ENTRYPOINT ["dbt"]
CMD ["--version"]
