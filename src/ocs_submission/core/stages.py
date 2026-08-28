"""Pipeline stage names used across OCS CLI and FASTQ record columns."""

from __future__ import annotations

from enum import Enum


class Stage(Enum):
    INGEST = ("ingest", "ingested-results")
    ALIGNMENT = ("align", "aligned-results")
    POST_ALIGNMENT = ("postalign", "post-aligned-results")

    def __init__(
        self,
        ocs_stage_name: str,
        ocs_list_results_arg: str,
    ) -> None:
        self.ocs_stage_name = ocs_stage_name
        self.ocs_list_results_arg = ocs_list_results_arg

    @property
    def fastq_status_column(self) -> str:
        return f"{self.ocs_stage_name}_status"


JOB_STAGES = (Stage.ALIGNMENT, Stage.POST_ALIGNMENT)
