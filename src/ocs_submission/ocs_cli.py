from __future__ import annotations

import json
import logging
import subprocess
import time
from datetime import datetime
from typing import cast

import pandas as pd

from .dats_pts import DatsPtsReader
from .stages import Stage

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


def execute_ocs_submission_commands(
    ocs_job_commands_df: pd.DataFrame,
    job_limit: int,
    poll_interval_hours: float = 1,
    dats_pts_reader: DatsPtsReader | None = None,
) -> pd.DataFrame:
    """
    Submit alignment or post-alignment jobs for rows with a true should-execute flag.

    Parameters:
    ocs_job_commands_df: A dataframe containing should-execute flags for each stage
        and FASTQ name.
    job_limit: The maximum number of active alignment and post-alignment PTS processes.
    poll_interval_hours: The number of hours to wait before checking PTS capacity again.
    dats_pts_reader: Optional reader for the PTS capacity check.

    Returns:
    The same dataframe with alignment and post-alignment demand id, success, error,
    and timestamp columns filled in for submitted jobs.
    """
    submit_indices = ocs_job_commands_df.index[
        ocs_job_commands_df["align_should_execute"] | ocs_job_commands_df["postalign_should_execute"]
    ]
    reserved_submissions = 0

    for record_index in submit_indices:
        if ocs_job_commands_df.at[record_index, "align_should_execute"]:
            stage = Stage.ALIGNMENT
        else:
            stage = Stage.POST_ALIGNMENT

        col = stage.ocs_stage_name
        dry_run = ocs_job_commands_df.at[record_index, "dry_run"]
        fastq_name = ocs_job_commands_df.at[record_index, "fastq_name"]
        command = ocs_job_commands_df.at[record_index, f"{col}_command"]
        command_args = cast(list[str], ocs_job_commands_df.at[record_index, f"{col}_command_args"])

        if dry_run:
            logger.info(f"Dry run {col} for {fastq_name}: {command}")
            continue

        if dats_pts_reader is None:
            dats_pts_reader = DatsPtsReader()
        while True:
            active_jobs = dats_pts_reader.count_active_submission_jobs()
            if active_jobs + reserved_submissions < job_limit:
                break
            logger.info(
                f"PTS job limit reached: {active_jobs} active jobs and {reserved_submissions} pending submissions "
                f"(limit: {job_limit}). Waiting {poll_interval_hours} hour(s) before checking again."
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
                reserved_submissions += 1
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
