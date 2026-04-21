"""Run planned OCS commands with limits, spacing, and logging."""

import logging
import time
from datetime import datetime

import pandas as pd

from . import running_jobs_db
from .ocs_cli import (
    can_submit_job,
    execute_ocs_cmd,
    extract_demand_id_from_output,
)

logger = logging.getLogger(__name__)


def execute_ocs_submission_commands(
    ocs_job_commands_df: pd.DataFrame, job_limit: int
) -> pd.DataFrame:
    """
    Submit the OCS commands for rows marked ``should_execute``.

    The function walks the frame in order. For each executable row it submits the command so
    long as the job limit has not been reached. Dry runs only log the command and mark the row
    successful. Real runs call the OCS CLI, capture the demand id, and register the job in
    ``running_jobs_db`` on success; failures are written back to ``error_message``. When a row
    carries a ``spacing`` value, the loop pauses that many seconds before the next submission.

    Parameters
    ----------
    ocs_job_commands_df
        Planned OCS job command rows; mutated in place with execution results.
    job_limit
        Maximum number of in-progress alignment plus post-alignment demands allowed.

    Return
    ----------
    pd.DataFrame
        Same frame with executed_at, demand_id, submission_success, output, and errors filled in.

    Pseudo code
    ----------
    for each (row, stage) where {prefix}_should_execute:
        if not can_submit_job(job_limit, dry_run): flip {prefix}_should_execute False; continue
        set {prefix}_executed_at
        if dry_run: log command; {prefix}_submission_success = True; continue
        try: run execute_ocs_cmd; parse demand id; update DB on success
        except: mark failure
        if not last executable and {prefix}_spacing: sleep({prefix}_spacing)
    return ocs_job_commands_df
    """
    stages = [("alignment", "alignment"), ("postqc", "post_alignment")]

    executable = [
        (record_index, job_type, prefix)
        for record_index in ocs_job_commands_df.index
        for job_type, prefix in stages
        if ocs_job_commands_df.at[record_index, f"{prefix}_should_execute"]
    ]

    for position, (record_index, job_type, prefix) in enumerate(executable):
        dry_run = bool(ocs_job_commands_df.at[record_index, "dry_run"])
        fastq_name = ocs_job_commands_df.at[record_index, "fastq_name"]
        command = ocs_job_commands_df.at[record_index, f"{prefix}_command"]
        command_args = ocs_job_commands_df.at[record_index, f"{prefix}_command_args"]

        if not can_submit_job(job_limit=job_limit, dry_run=dry_run):
            ocs_job_commands_df.at[record_index, f"{prefix}_should_execute"] = False
            continue

        ocs_job_commands_df.at[record_index, f"{prefix}_executed_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        if dry_run:
            logger.info("Dry run %s for %s: %s", job_type, fastq_name, command)
            ocs_job_commands_df.at[record_index, f"{prefix}_submission_success"] = True
            continue

        logger.info("Submitting %s for %s: %s", job_type, fastq_name, command)

        try:
            result = execute_ocs_cmd(command_args)
            output = result.stdout
            ocs_job_commands_df.at[record_index, f"{prefix}_output"] = output

            demand_id, submission_success = extract_demand_id_from_output(output)
            ocs_job_commands_df.at[record_index, f"{prefix}_demand_id"] = demand_id
            ocs_job_commands_df.at[record_index, f"{prefix}_submission_success"] = (
                submission_success
            )

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
                ocs_job_commands_df.at[record_index, f"{prefix}_error_message"] = (
                    "Job submission failed"
                )
                logger.error("Job submission failed")
        except Exception as error:
            ocs_job_commands_df.at[record_index, f"{prefix}_submission_success"] = False
            ocs_job_commands_df.at[record_index, f"{prefix}_error_message"] = (
                f"Command execution failed: {error}"
            )
            logger.error("Command execution failed: %s", error)

        is_last_executable = position == len(executable) - 1
        spacing = ocs_job_commands_df.at[record_index, f"{prefix}_spacing"]
        if not is_last_executable and spacing:
            time.sleep(spacing)

    return ocs_job_commands_df
