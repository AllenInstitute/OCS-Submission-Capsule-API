from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import time
from datetime import datetime
from typing import Any, cast

import pandas as pd

from ..core.stages import Stage
from . import running_jobs_db

logger = logging.getLogger(__name__)


def execute_ocs_cmd(cmd_list: list[str]) -> subprocess.CompletedProcess:
    """
    Execute an OCS CLI command and return its output.

    Parameters:
    cmd_list: A list of strings representing the OCS CLI command to execute.

    Returns:
    A subprocess.CompletedProcess object containing the output of the OCS CLI command.
    """

    return subprocess.run(cmd_list, check=True, capture_output=True, text=True)


def extract_demand_id_from_output(output_text: str) -> tuple[str | None, bool]:
    """
    Parse the demand ID from an OCS CLI response.

    Parameters:
    output_text: A string containing the output of an OCS CLI command.

    Returns:
    A tuple containing the demand ID and a boolean indicating whether the demand was
    submitted successfully.
    """
    json_output = json.loads(output_text)
    if json_output.get("demand_status") == "SUBMITTED":
        demand_execution = json_output.get("demand_execution")
        demand_id = demand_execution.get("demand_id")
        return demand_id, True

    return None, False


def count_jobs(job_type: str) -> int:
    """
    Return the number of in-progress alignment or post-alignment jobs.

    Parameters:
    job_type: The demand type to count, either ``align`` or ``post-align``.

    Returns:
    The number of in-progress OCS demands of the given demand type.
    """
    cmd = [
        "ocs",
        "core",
        "gwo",
        "demand",
        "list-demands",
        "--demand-type",
        job_type,
        "--status",
        "IN_PROGRESS",
        "--format",
        "json",
    ]
    result = execute_ocs_cmd(cmd_list=cmd).stdout.strip()

    if result and "No demands were found" not in result:
        return len(json.loads(result))
    return 0


def can_submit_job(job_limit: int, dry_run: bool = False) -> bool:
    """
    Check whether a new OCS job fits within the configured limit.

    Parameters:
    job_limit: The maximum number of in-progress alignment and post-alignment jobs
        allowed before new submissions are blocked.
    dry_run: A boolean indicating whether to perform a dry run

    Returns:
    ``True`` when a new OCS job would stay within the configured limit.
    """
    if dry_run:
        return True

    align_count = count_jobs("align")
    post_align_count = count_jobs("post-align")
    total_jobs = align_count + post_align_count

    if total_jobs >= job_limit:
        logger.info(f"Cannot submit job: {total_jobs} jobs already running (limit: {job_limit})")
        logger.info(f"  - Alignment jobs: {align_count}")
        logger.info(f"  - Post-alignment jobs: {post_align_count}")
        return False

    return True


def get_latest_results(
    fastq_name_list: list[str] | None = None,
    batch_name_from_vendor: str | None = None,
) -> pd.DataFrame:
    """
    Return the latest OCS result for FASTQ names or a vendor batch.

    Parameters:
    fastq_name_list: FASTQ names whose OCS status should be checked.
    batch_name_from_vendor: Vendor batch whose OCS status should be checked.

    Returns:
    A dataframe with one row per fastq name and ingest_status, align_status, and
    postalign_status columns set to COMPLETED or NOT COMPLETED.
    """

    def list_results_cmd(results_arg: str) -> list[str]:
        return [
            "ocs",
            "fastqs",
            "list",
            results_arg,
            "--latest",
            "--detail",
            "--format",
            "json",
        ]

    def status_from_entry(entry: dict[str, Any]) -> str:
        return "COMPLETED" if entry.get("fastq_results") else "NOT COMPLETED"

    if fastq_name_list:
        status_columns = [stage.fastq_status_column for stage in Stage]
        fastq_stage_status_df = pd.DataFrame("NOT COMPLETED", index=fastq_name_list, columns=status_columns)
        fastq_stage_status_df.index.name = "fastq_name"

        async def fetch(stage: Stage, fastq_name: str) -> tuple[str, str, str]:
            cmd = list_results_cmd(stage.ocs_list_results_arg) + ["--fastq-name", fastq_name]
            stdout = (await asyncio.to_thread(execute_ocs_cmd, cmd)).stdout
            entries = json.loads(stdout)
            return fastq_name, stage.fastq_status_column, status_from_entry(entries[0])

        async def fill_results() -> None:
            results = await asyncio.gather(
                *(fetch(stage, fastq_name) for stage in Stage for fastq_name in fastq_name_list)
            )
            for fastq_name, status_column, status in results:
                fastq_stage_status_df.at[fastq_name, status_column] = status

        asyncio.run(fill_results())
        return fastq_stage_status_df
    else:
        if batch_name_from_vendor is None:
            raise ValueError("get_latest_results requires fastq_name_list or batch_name_from_vendor")

        status_columns = [stage.fastq_status_column for stage in Stage]
        fastq_stage_status_df = pd.DataFrame(columns=status_columns)
        fastq_stage_status_df.index.name = "fastq_name"

        for stage in Stage:
            cmd = list_results_cmd(stage.ocs_list_results_arg) + [
                "--batch-name-from-vendor",
                batch_name_from_vendor,
            ]
            entries = json.loads(execute_ocs_cmd(cmd_list=cmd).stdout.strip())
            for entry in entries:
                fastq_name = entry["fastq_name"]
                fastq_stage_status_df.at[fastq_name, stage.fastq_status_column] = status_from_entry(entry)

        return fastq_stage_status_df


def query_metadata(
    fastq_name_list: list[str] | None = None,
    batch_name_from_vendor: str | None = None,
) -> pd.DataFrame:
    """
    Return OCS metadata for FASTQ names or a vendor batch.

    Raises ``ValueError`` unless exactly one lookup input is provided.

    Parameters:
    fastq_name_list: A list of fastq names to query metadata for.
    batch_name_from_vendor: A batch name from vendor to query metadata for.

    Returns:
    A dataframe with the index set to the fastq name and the columns set to the metadata fields.
    """

    has_fastq_lookup = bool(fastq_name_list)
    has_batch_lookup = bool(batch_name_from_vendor)
    if has_fastq_lookup == has_batch_lookup:
        raise ValueError("query_metadata requires exactly one of fastq_name_list or batch_name_from_vendor")

    metadata_base_cmd = [
        "ocs",
        "fastqs",
        "list",
        "metadata",
        "--include-metadata-field",
        "organism_common_name",
        "--include-metadata-field",
        "library_prep_method_name",
        "--include-metadata-field",
        "studies",
        "--include-metadata-field",
        "load_name",
        "--include-metadata-field",
        "batch_name_from_vendor",
        "--format",
        "json",
    ]

    all_metadata_rows: list[dict[str, Any]] = []
    if fastq_name_list:
        for fastq_name in fastq_name_list:
            metadata_cmd = metadata_base_cmd + ["--fastq-name", fastq_name]
            metadata_rows = json.loads(execute_ocs_cmd(cmd_list=metadata_cmd).stdout)
            if not metadata_rows:
                raise ValueError(
                    f"OCS returned no metadata for fastq {fastq_name!r}. "
                    "There may be an issue with this FASTQ on OCS — verify metadata with "
                    "`ocs fastqs list metadata` and perform a manual check."
                )
            all_metadata_rows.extend(metadata_rows)
    else:
        if batch_name_from_vendor is None:
            raise ValueError("query_metadata requires fastq_name_list or batch_name_from_vendor")

        metadata_cmd = metadata_base_cmd + [
            "--batch-name-from-vendor",
            batch_name_from_vendor,
        ]
        all_metadata_rows = json.loads(execute_ocs_cmd(cmd_list=metadata_cmd).stdout)
        if not all_metadata_rows:
            raise ValueError(
                f"OCS returned no metadata for batch {batch_name_from_vendor!r}. "
                "There may be an issue with this batch on OCS — verify metadata with "
                "`ocs fastqs list metadata` and perform a manual check."
            )

    metadata_df = pd.DataFrame(all_metadata_rows)
    metadata_df["study_set"] = metadata_df["studies"].apply(
        lambda studies: "+".join(studies) if isinstance(studies, list) else str(studies)
    )
    return metadata_df.set_index("fastq_name", drop=False)


def execute_ocs_submission_commands(
    ocs_job_commands_df: pd.DataFrame, job_limit: int, poll_interval_hours: float = 1
) -> pd.DataFrame:
    """
    Submit alignment or post-alignment jobs for rows with a true should-execute flag.

    Submission waits when the job limit is reached.

    Parameters:
    ocs_job_commands_df: A dataframe containing should-execute flags for each stage
        and FASTQ name.
    job_limit: The maximum number of jobs allowed to be running at OCS.
    poll_interval_hours: The number of hours to wait between checking if the job limit is reached.

    Returns:
    The same dataframe with alignment and post-alignment demand id, success, error,
    and timestamp columns filled in for submitted jobs.
    """
    submit_indices = ocs_job_commands_df.index[
        ocs_job_commands_df["align_should_execute"] | ocs_job_commands_df["postalign_should_execute"]
    ]

    for record_index in submit_indices:
        if ocs_job_commands_df.at[record_index, "align_should_execute"]:
            stage = Stage.ALIGNMENT
        else:
            stage = Stage.POST_ALIGNMENT

        col = stage.ocs_stage_name
        dry_run = cast(bool, ocs_job_commands_df.at[record_index, "dry_run"])
        fastq_name = cast(str, ocs_job_commands_df.at[record_index, "fastq_name"])
        command = cast(str, ocs_job_commands_df.at[record_index, f"{col}_command"])
        command_args = cast(list[str], ocs_job_commands_df.at[record_index, f"{col}_command_args"])

        if dry_run:
            logger.info(f"Dry run {col} for {fastq_name}: {command}")
            continue

        while not can_submit_job(job_limit=job_limit, dry_run=dry_run):
            logger.info(
                f"Job limit reached; waiting {poll_interval_hours} hour(s) "
                f"before re-checking capacity for {fastq_name} ({col})."
            )
            time.sleep(poll_interval_hours * 3600)

        ocs_job_commands_df.at[record_index, f"{col}_executed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Submitting {col} for {fastq_name}: {command}")

        try:
            result = execute_ocs_cmd(command_args)
            demand_id, submission_success = extract_demand_id_from_output(result.stdout)
            ocs_job_commands_df.at[record_index, f"{col}_demand_id"] = demand_id
            ocs_job_commands_df.at[record_index, f"{col}_submission_success"] = submission_success

            if submission_success and demand_id:
                running_db_stage_name = stage.running_db_stage_name
                if running_db_stage_name is None:
                    raise ValueError(f"Stage {stage.name} is not tracked in the running-jobs database")

                running_jobs_db.add_job(
                    fastq_name=fastq_name,
                    running_db_stage_name=running_db_stage_name,
                    command=command,
                    demand_id=demand_id,
                    batch_name_from_vendor=cast(
                        str | None,
                        ocs_job_commands_df.at[record_index, "batch_name_from_vendor"],
                    ),
                )
                logger.info(f"Job submitted successfully - Demand ID: {demand_id}")
            else:
                ocs_job_commands_df.at[record_index, f"{col}_error_message"] = "Job submission failed"
                logger.error("Job submission failed")
        except Exception as error:
            ocs_job_commands_df.at[record_index, f"{col}_submission_success"] = False
            ocs_job_commands_df.at[record_index, f"{col}_error_message"] = f"Command execution failed: {error}"
            logger.error(f"Command execution failed: {error}")

        spacing = cast(float, ocs_job_commands_df.at[record_index, f"{col}_spacing"])
        if spacing and record_index != submit_indices[-1]:
            time.sleep(spacing)

    return ocs_job_commands_df
