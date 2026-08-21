"""Read FASTQ processes and assets from PTS and DATS."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from allen_services_api.dats.client.dats_client import DatsClient
from allen_services_api.pts.client.pts_client import PtsClient
from allen_services_api.pts.schema import pts_schema as schema

from .stages import Stage

PROCESS_TYPES = {
    Stage.INGEST: "prod-ocs-ingest",
    Stage.ALIGNMENT: "prod-ocs-align",
    Stage.POST_ALIGNMENT: "prod-ocs-post-align",
}
ACTIVE_STATES = {"PENDING", "QUEUED", "RUNNING", "IN_PROGRESS"}
SUBMISSION_STAGES = (Stage.ALIGNMENT, Stage.POST_ALIGNMENT)


@dataclass(frozen=True)
class StageResult:
    status: str
    location: str | None


class DatsPtsReader:
    """Resolve FASTQ metadata, process state, and output locations."""

    def __init__(self) -> None:
        if not os.environ.get("ALLEN_SERVICE_API_CONFIG"):
            raise ValueError("ALLEN_SERVICE_API_CONFIG must be set")
        self.pts = PtsClient()
        self.dats = DatsClient()

    def get_stage_result(self, fastq_name: str, stage: Stage) -> StageResult:
        return self.get_stage_results(fastq_name)[stage]

    def count_active_submission_jobs(self) -> int:
        """Return the number of active alignment and post-alignment PTS processes."""
        active_job_count = 0
        for stage in SUBMISSION_STAGES:
            process_type = self._process_type(PROCESS_TYPES[stage])
            for state in ACTIVE_STATES:
                active_job_count += self.pts.get_processes_by_type(process_type, state=state, first=1).total_count
        return active_job_count

    def get_stage_results(self, fastq_name: str) -> dict[Stage, StageResult]:
        ingest_processes = self._processes_for_fastq(fastq_name, Stage.INGEST)
        ingest_process = self._select_process(ingest_processes)
        alignment_processes = self._downstream_processes(
            [ingest_process] if ingest_process else [], Stage.ALIGNMENT, fastq_name
        )
        alignment_process = self._select_process(alignment_processes)
        post_alignment_process = self._select_process(
            self._downstream_processes(
                [alignment_process] if alignment_process else [], Stage.POST_ALIGNMENT, fastq_name
            )
        )
        processes = {
            Stage.INGEST: ingest_process,
            Stage.ALIGNMENT: alignment_process,
            Stage.POST_ALIGNMENT: post_alignment_process,
        }
        results: dict[Stage, StageResult] = {}
        for stage, process in processes.items():
            if process is None:
                results[stage] = StageResult(status="NOT COMPLETED", location=None)
                continue
            status = self._status(process)
            location = self._location(process, fastq_name)
            if status == "COMPLETED" and location is None:
                raise ValueError(f"Completed PTS process has no DATS location for {fastq_name!r} ({stage.value})")
            results[stage] = StageResult(status=status, location=location)
        return results

    def _processes_for_fastq(self, fastq_name: str, stage: Stage) -> list[Any]:
        process_type = self._process_type(PROCESS_TYPES[stage])
        processes = self.pts.get_processes_by_metadata(
            schema.MetadataFilterInput(data=schema.JsonOperationFilterInput(contains={"fastq_name": fastq_name})),
            first=50,
        ).nodes
        return [process for process in processes if str(process.type.id) == str(process_type.id)]

    def _process_type(self, name: str) -> Any:
        process_types = self.pts.get_process_types_by_name(name, version=1.0).nodes
        if len(process_types) != 1:
            raise ValueError(f"Expected one PTS process type {name!r} version 1.0")
        return process_types[0]

    def _select_process(self, processes: list[Any]) -> Any:
        if not processes:
            return None
        return max(processes, key=lambda process: (process.created_at, str(process.id)))

    def _downstream_processes(self, processes: list[Any], target_stage: Stage, fastq_name: str) -> list[Any]:
        candidates: list[Any] = []
        target_type = self._process_type(PROCESS_TYPES[target_stage])
        asset_ids = [
            output.external_id
            for process in processes
            for output in self.pts.get_process_outputs(process.id)
            if str(getattr(output.type, "name", output.type)) == "DIGITAL_ASSET"
        ]
        if not asset_ids:
            return candidates
        assets = self.dats.get_assets_by_ids(asset_ids, first=50).nodes
        asset_ids = [str(asset.id) for asset in assets if f"fastq_name: {fastq_name}" in (asset.tags or [])]
        if not asset_ids:
            return candidates
        downstream = self.pts.get_processes_by_assets(
            asset_ids, relationship=schema.ProcessAssetRelationship("INPUT"), first=50
        ).nodes
        candidates.extend(process for process in downstream if str(process.type.id) == str(target_type.id))
        return candidates

    @staticmethod
    def _status(process: Any) -> str:
        state = str(getattr(process.state, "name", process.state)).upper()
        if state == "SUCCESS":
            return "COMPLETED"
        if state in ACTIVE_STATES:
            return "IN_PROGRESS"
        return state

    def _location(self, process: Any, fastq_name: str) -> str | None:
        asset_ids = [
            output.external_id
            for output in self.pts.get_process_outputs(process.id)
            if str(getattr(output.type, "name", output.type)) == "DIGITAL_ASSET"
        ]
        if not asset_ids:
            return None
        assets = self.dats.get_assets_by_ids(asset_ids, first=50).nodes
        matching_instances = [
            instance.download_url
            for asset in assets
            if f"fastq_name: {fastq_name}" in (asset.tags or [])
            for instance in (asset.instances or [])
            if instance.download_url
        ]
        locations = sorted(set(matching_instances))
        if len(locations) > 1:
            raise ValueError(f"Multiple DATS locations found for {fastq_name!r}")
        return locations[0] if locations else None
