from __future__ import annotations

import json
import logging
import subprocess
import time
from datetime import datetime
from typing import Any

import pandas as pd

from . import running_jobs_db

logger = logging.getLogger(__name__)


def execute_ocs_cmd(cmd_list: list[str]) -> subprocess.CompletedProcess:
    """Run the OCS CLI as a subprocess and return the completed process object."""
    return subprocess.run(cmd_list, check=True, capture_output=True, text=True)


def extract_demand_id_from_output(output_text: str) -> tuple[str | None, bool]:
    """
    Parse the JSON that OCS prints after a demand submission and pull out the demand id.

    Expects an object with a ``demand_status`` field plus ``demand_execution.demand_id`` when
    the submission succeeded.

    Parameters
    ----------
    output_text
        Raw stdout string from the OCS demand submission command.

    Return
    ----------
    tuple[str | None, bool]
        ``(demand_id, True)`` when status is SUBMITTED and an id is present; otherwise
        ``(None, False)``.

    """
    json_output = json.loads(output_text)
    if json_output.get("demand_status") == "SUBMITTED":
        demand_execution = json_output.get("demand_execution") or {}
        demand_id = demand_execution.get("demand_id")
        return demand_id, True

    return None, False


def count_jobs(job_type: str) -> int:
    """
    Count in-progress OCS demands of a given demand type.

    Calls ``ocs core gwo demand list-demands`` filtered to ``IN_PROGRESS`` and returns the number
    of rows in the JSON response.

    Parameters
    ----------
    job_type
        Demand type string passed to ``--demand-type`` (for example ``align`` or ``post-align``).

    Return
    ----------
    int
        Number of matching in-progress demands, or 0 if none or if the CLI reports none.

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
    Check whether a new OCS job can be submitted without exceeding the running-job limit.

    Dry runs always return ``True`` because nothing is actually submitted.

    Parameters
    ----------
    job_limit
        Maximum number of in-progress alignment plus post-alignment demands allowed before
        blocking new submissions.
    dry_run
        When True, skip counting and allow submission (for print-only runs).

    Return
    ----------
    bool
        True if submission is allowed, False if the running total is already at or above
        ``job_limit``.

    """
    if dry_run:
        return True

    align_count = count_jobs("align")
    post_align_count = count_jobs("post-align")
    total_jobs = align_count + post_align_count

    if total_jobs >= job_limit:
        logger.info(
            "Cannot submit job: %s jobs already running (limit: %s)",
            total_jobs,
            job_limit,
        )
        logger.info("  - Alignment jobs: %s", align_count)
        logger.info("  - Post-alignment jobs: %s", post_align_count)
        return False

    return True


def get_latest_results(
    stage: str,
    fastq_name: str | None = None,
    batch_name_from_vendor: str | None = None,
) -> dict[str, Any] | dict[str, dict[str, Any]]:
    """
    Fetch the latest OCS result entry for a stage.

    Two lookup modes are supported:
    - by ``fastq_name``: returns the latest result dict for that one FASTQ
    - by ``batch_name_from_vendor``: returns a mapping of ``fastq_name`` to latest result dict

    Uses ``ocs fastqs list ... --latest --detail --format json`` and reads the first result entry
    out of the response.

    Parameters
    ----------
    stage
        Pipeline stage to query. Expected values are ``ingest``, ``align``, or ``post-align``.
    fastq_name
        FASTQ name to query. When provided, the function returns a single latest result dict or
        ``None``.
    batch_name_from_vendor
        Batch name from vendor to query. When provided, the function returns latest results for all
        FASTQs in the batch.

    Return
    ----------
    dict[str, Any] | dict[str, dict[str, Any]]
        If ``fastq_name`` is provided, returns the latest result dict for that FASTQ, or ``None``
        when no result is available. If ``batch_name_from_vendor`` is provided, returns a mapping
        of ``fastq_name`` to latest result dict.

    """
    endpoint_map = {
        "ingest": "ingested-results",
        "align": "aligned-results",
        "post-align": "post-aligned-results",
    }

    cmd = [
        "ocs",
        "fastqs",
        "list",
        endpoint_map[stage],
        "--latest",
        "--detail",
        "--format",
        "json",
    ]
    if fastq_name:
        cmd.extend(["--fastq-name", fastq_name])
    else:
        cmd.extend(["--batch-name-from-vendor", batch_name_from_vendor])

    result = execute_ocs_cmd(cmd_list=cmd).stdout.strip()

    if not result:
        return None if fastq_name else {}

    stage_results = json.loads(result)
    if fastq_name:
        try:
            return stage_results[0]["fastq_results"][0]["result"][0]
        except (KeyError, IndexError, TypeError):
            return None

    latest_results_by_fastq = {}
    for stage_result in stage_results:
        try:
            latest_results_by_fastq[stage_result["fastq_name"]] = stage_result["fastq_results"][0]["result"][0]
        except (KeyError, IndexError, TypeError):
            pass
    return latest_results_by_fastq


def get_status(latest_entry: dict[str, Any] | None, status_type: str) -> str:
    """
    Look up the current OCS status for a latest result entry.

    Uses the stage-specific ``ocs fastqs <stage> get-status`` command, keyed on ``demand_id``.
    Returns ``NOT COMPLETED`` when the entry is missing, has no demand id, or OCS does not return
    a status.

    Parameters
    ----------
    latest_entry
        Latest result dict for the stage, or ``None``.
    status_type
        Stage name used to choose the OCS get-status subcommand. Expected values are ``ingest``,
        ``align``, or ``post-align``.

    Return
    ----------
    str
        Status string returned by OCS, or ``NOT COMPLETED`` when no status can be resolved.

    """
    if not latest_entry:
        return "NOT COMPLETED"

    endpoint_map = {"ingest": "ingest", "align": "align", "post-align": "postalign"}
    demand_id = latest_entry.get("demand_id")
    if not demand_id or demand_id == "null":
        return "NOT COMPLETED"

    cmd = [
        "ocs",
        "fastqs",
        endpoint_map[status_type],
        "get-status",
        "--demand-id",
        demand_id,
        "--format",
        "json",
    ]
    result = execute_ocs_cmd(cmd_list=cmd).stdout.strip()
    if not result:
        return "NOT COMPLETED"

    status_data = json.loads(result)
    if not status_data:
        return "NOT COMPLETED"

    return status_data[0].get("status") or "NOT COMPLETED"


def query_metadata(
    fastq_name: str | None = None,
    batch_name_from_vendor: str | None = None,
) -> pd.DataFrame:
    """
    Query FASTQ metadata from OCS and return it as a dataframe.

    Accepts either a single ``fastq_name`` or a ``batch_name_from_vendor``. The JSON response is
    loaded straight into a dataframe, and ``study_set`` is derived from ``studies`` (joined with
    ``+``) so downstream code can use it directly.

    Parameters
    ----------
    fastq_name
        FASTQ name to query. When provided, the result is typically a one-row dataframe.
    batch_name_from_vendor
        Batch name from vendor to query. When provided, the result includes one row per FASTQ in the
        batch.

    Return
    ----------
    pd.DataFrame
        Dataframe of metadata rows indexed by ``fastq_name`` while also keeping ``fastq_name`` as
        a column. The frame also includes a derived ``study_set`` column.

    Raises
    ----------
    ValueError
        If OCS returns no metadata rows for the requested FASTQ or batch.

    """
    if not fastq_name and not batch_name_from_vendor:
        raise ValueError(
            "query_metadata requires fastq_name or batch_name_from_vendor"
        )

    metadata_cmd = [
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
    if fastq_name:
        metadata_cmd.extend(["--fastq-name", fastq_name])
    else:
        metadata_cmd.extend(["--batch-name-from-vendor", batch_name_from_vendor])

    metadata_rows = json.loads(execute_ocs_cmd(cmd_list=metadata_cmd).stdout)
    if not metadata_rows:
        if fastq_name:
            raise ValueError(
                f"OCS returned no metadata for fastq {fastq_name!r}. "
                "There may be an issue with this FASTQ on OCS — verify metadata with "
                "`ocs fastqs list metadata` and perform a manual check."
            )
        raise ValueError(
            f"OCS returned no metadata for batch {batch_name_from_vendor!r}. "
            "There may be an issue with this batch on OCS — verify metadata with "
            "`ocs fastqs list metadata` and perform a manual check."
        )

    metadata_df = pd.DataFrame(metadata_rows)
    metadata_df["study_set"] = metadata_df["studies"].apply(
        lambda studies: "+".join(studies) if isinstance(studies, list) else str(studies)
    )
    return metadata_df.set_index("fastq_name", drop=False)


def execute_ocs_submission_commands(
    ocs_job_commands_df: pd.DataFrame, job_limit: int, poll_interval_hours: float = 1
) -> pd.DataFrame:
    """Submit planned OCS commands for rows marked should_execute.

    When the running-job limit is reached, submission does not skip the job. Instead it blocks,
    re-checking capacity every ``poll_interval_hours`` hours until a slot frees up, then submits
    and moves on to the next job. Dry-run rows never block because ``can_submit_job`` allows them
    unconditionally.

    Parameters
    ----------
    ocs_job_commands_df
        Planned OCS commands, one row per FASTQ, with per-stage ``*_should_execute`` flags.
    job_limit
        Maximum number of in-progress alignment plus post-alignment demands allowed.
    poll_interval_hours
        Hours to wait between capacity checks when the limit has been reached.
    """
    executable_list = [
        (record_index, job_type, prefix)
        for record_index in ocs_job_commands_df.index
        for job_type, prefix in [("alignment", "alignment"), ("postqc", "post_alignment")]
        if ocs_job_commands_df.at[record_index, f"{prefix}_should_execute"]
    ]

    for position, (record_index, job_type, prefix) in enumerate(executable_list):
        dry_run = ocs_job_commands_df.at[record_index, "dry_run"]
        fastq_name = ocs_job_commands_df.at[record_index, "fastq_name"]
        command = ocs_job_commands_df.at[record_index, f"{prefix}_command"]
        command_args = ocs_job_commands_df.at[record_index, f"{prefix}_command_args"]

        while not can_submit_job(job_limit=job_limit, dry_run=dry_run):
            logger.info(
                "Job limit reached; waiting %s hour(s) before re-checking capacity for %s (%s).",
                poll_interval_hours,
                fastq_name,
                job_type,
            )
            time.sleep(poll_interval_hours * 3600)

        if dry_run:
            logger.info("Dry run %s for %s: %s", job_type, fastq_name, command)
            continue

        ocs_job_commands_df.at[record_index, f"{prefix}_executed_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        logger.info("Submitting %s for %s: %s", job_type, fastq_name, command)

        try:
            result = execute_ocs_cmd(command_args)
            demand_id, submission_success = extract_demand_id_from_output(result.stdout)
            ocs_job_commands_df.at[record_index, f"{prefix}_demand_id"] = demand_id
            ocs_job_commands_df.at[record_index, f"{prefix}_submission_success"] = submission_success

            if submission_success and demand_id:
                running_jobs_db.add_job(
                    fastq_name=fastq_name,
                    job_type=job_type,
                    command=command,
                    demand_id=demand_id,
                    batch_name_from_vendor=ocs_job_commands_df.at[
                        record_index, "batch_name_from_vendor"
                    ],
                )
                logger.info("Job submitted successfully - Demand ID: %s", demand_id)
            else:
                ocs_job_commands_df.at[record_index, f"{prefix}_error_message"] = "Job submission failed"
                logger.error("Job submission failed")
        except Exception as error:
            ocs_job_commands_df.at[record_index, f"{prefix}_submission_success"] = False
            ocs_job_commands_df.at[record_index, f"{prefix}_error_message"] = f"Command execution failed: {error}"
            logger.error("Command execution failed: %s", error)

        is_last_executable = position == len(executable_list) - 1
        spacing = ocs_job_commands_df.at[record_index, f"{prefix}_spacing"]
        if not is_last_executable and spacing:
            time.sleep(spacing)

    return ocs_job_commands_df
