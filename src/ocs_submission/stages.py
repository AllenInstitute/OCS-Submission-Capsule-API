"""Pipeline stage names used across OCS submissions and FASTQ record columns."""

from __future__ import annotations

from enum import Enum


class Stage(Enum):
    INGEST = "ingest"
    ALIGNMENT = "align"
    POST_ALIGNMENT = "postalign"

    def __init__(self, ocs_stage_name: str) -> None:
        self.ocs_stage_name = ocs_stage_name

    @property
    def fastq_status_column(self) -> str:
        return f"{self.ocs_stage_name}_status"
