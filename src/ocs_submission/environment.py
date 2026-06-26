"""Environment variables for the OCS submission capsule."""

from __future__ import annotations

import os

AWS_CREDENTIAL_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
)


def running_jobs_db_url() -> str:
    return os.environ["RUNNING_JOBS_DB_URL"]


def lims_database_username() -> str:
    return os.environ["DATABASE_USERNAME"]


def lims_database_password() -> str:
    return os.environ["DATABASE_PASSWORD"]


def clear_aws_credential_env() -> None:
    for env_key in AWS_CREDENTIAL_ENV_KEYS:
        os.environ.pop(env_key, None)
