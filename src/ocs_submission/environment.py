"""Environment variables for the OCS submission capsule."""

from __future__ import annotations

import os

AWS_CREDENTIAL_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
)


def clear_aws_credential_env() -> None:
    for env_key in AWS_CREDENTIAL_ENV_KEYS:
        os.environ.pop(env_key, None)
