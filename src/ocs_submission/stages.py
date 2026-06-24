"""Pipeline stage names used across OCS CLI, tracker DB, and FASTQ record columns."""

from __future__ import annotations

from enum import Enum


class Stage(Enum):
    INGEST = ("ingest", "ingested-results", None)
    ALIGNMENT = ("align", "aligned-results", "alignment")
    POST_ALIGNMENT = ("postalign", "post-aligned-results", "postqc")

    def __init__(
        self,
        ocs_stage_name: str,
        ocs_list_results_arg: str,
        running_db_stage_name: str | None,
    ) -> None:
        self.ocs_stage_name = ocs_stage_name
        self.ocs_list_results_arg = ocs_list_results_arg
        self.running_db_stage_name = running_db_stage_name

    @property
    def fastq_status_column(self) -> str:
        return f"{self.ocs_stage_name}_status"
