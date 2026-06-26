"""Email summaries after command execution."""

from __future__ import annotations

import logging
import os
from datetime import datetime

import boto3
import pandas as pd

from . import OUTPUT_DIR
from .audit import run_audit

logger = logging.getLogger(__name__)
REGION = os.environ["REGION"]
SOURCE = os.environ["SOURCE"]


def send_email(to_address: str, subject: str, body: str) -> str:
    for env_key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
    ):
        os.environ.pop(env_key, None)

    ses = boto3.client("ses", region_name=REGION)
    response = ses.send_email(
        Source=SOURCE,
        Destination={"ToAddresses": [to_address]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body, "Charset": "UTF-8"},
            },
        },
    )
    return response["MessageId"]


def _stage_outcome(record, prefix: str) -> dict | None:
    """
    Read one stage's execution outcome from a command record into a flat dict.

    Returns ``None`` when the stage was not executed (submission_success is ``None``). Otherwise
    returns a dict with the identity fields and either demand id (success) or error message
    (failure), ready to be formatted by ``_format_block``.

    Parameters
    ----------
    record
        Named tuple row from ``ocs_job_commands_df.itertuples``.
    prefix
        Stage prefix, either ``"alignment"`` or ``"post_alignment"``.

    Return
    ----------
    dict or None
        Outcome dict, or ``None`` when no submission was attempted for this stage.
    """
    submission_success = getattr(record, f"{prefix}_submission_success")
    if submission_success is None:
        return None
    return {
        "success": submission_success,
        "time": getattr(record, f"{prefix}_executed_at"),
        "fastq_name": record.fastq_name,
        "load_name": record.load_name,
        "command": getattr(record, f"{prefix}_command"),
        "demand_id": getattr(record, f"{prefix}_demand_id"),
        "error_message": getattr(record, f"{prefix}_error_message"),
    }


def _format_block(index: int, job_type: str, outcome: dict) -> str:
    """
    Format one submission or failure block for the summary email body.

    Success blocks include the demand id; failure blocks include the error message. Both include
    the common identity fields and the command that was submitted.

    Parameters
    ----------
    index
        1-based position of this entry in its section.
    job_type
        Stage label displayed in the email (``"alignment"`` or ``"postqc"``).
    outcome
        Dict produced by ``_stage_outcome``.

    Return
    ----------
    str
        Multi-line block terminated by a trailing newline.
    """
    line_list = [
        f"{index}. Fastq Name: {outcome['fastq_name']}",
        f"   Load Name: {outcome['load_name']}",
        f"   Job Type: {job_type}",
        f"   Time: {outcome['time']}",
        f"   Command: {outcome['command']}",
    ]
    if outcome["success"]:
        line_list.insert(3, f"   Demand ID: {outcome['demand_id']}")
    else:
        line_list.insert(3, f"   Error: {outcome['error_message']}")
    return "\n".join(line_list) + "\n"


def send_command_summary_email(
    ocs_job_commands_df: pd.DataFrame, notify_email: str
) -> None:
    """
    Email a summary of submissions and failures after execution.

    Does nothing if ``notify_email`` is empty or the frame is empty. Dry-run rows are skipped.
    Walks each row and each stage, routing stage outcomes into success and failure lists, then
    builds a plain-text email body and sends it via AWS SES. If the frame carries
    exactly one batch name, that batch is used in the subject line.

    Parameters
    ----------
    ocs_job_commands_df
        Post-execution OCS job command rows (one per FASTQ, with ``alignment_*`` and
        ``post_alignment_*`` columns).
    notify_email
        Destination address; if empty, no email is sent.

    Return
    ----------
    None
    """
    if not notify_email or ocs_job_commands_df.empty:
        return

    success_list: list[tuple[str, dict]] = list()
    failure_list: list[tuple[str, dict]] = list()
    for record in ocs_job_commands_df.itertuples(index=False):
        if record.dry_run:
            continue
        for job_type, prefix in [("alignment", "alignment"), ("postqc", "post_alignment")]:
            outcome = _stage_outcome(record, prefix)
            if outcome is None:
                continue
            (success_list if outcome["success"] else failure_list).append((job_type, outcome))

    if not (success_list or failure_list):
        return

    batches = ocs_job_commands_df["batch_name_from_vendor"].dropna().unique()
    batch_label = batches[0] if len(batches) == 1 else None
    subject = (
        f"OCS Job Submission Summary - Batch: {batch_label}"
        if batch_label
        else "OCS Job Submission Summary"
    )

    body_part_list = [
        "OCS Submission Summary Notification",
        "===================================",
        "",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total Jobs Processed: {len(success_list) + len(failure_list)}",
        f"Successful Submissions: {len(success_list)}",
        f"Failed Submissions: {len(failure_list)}",
    ]
    if batch_label:
        body_part_list.append(f"Batch Name: {batch_label}")

    if success_list:
        body_part_list.extend(["", "Successful Submissions:", "-" * 50])
        body_part_list.extend(
            _format_block(i, job_type, outcome)
            for i, (job_type, outcome) in enumerate(success_list, 1)
        )

    if failure_list:
        body_part_list.extend(["", "Failed Submissions:", "-" * 50])
        body_part_list.extend(
            _format_block(i, job_type, outcome)
            for i, (job_type, outcome) in enumerate(failure_list, 1)
        )

    body_part_list.append("")
    body_part_list.append("This is an automated notification from the OCS Submission Capsule")

    message_id = send_email(
        to_address=notify_email,
        subject=subject,
        body="\n".join(body_part_list),
    )
    logger.info("Email sent via SES. Message ID: %s", message_id)


def send_audit_email(batch_name_from_vendor: str, notify_email: str) -> None:
    """
    Run the LIMS audit for ``batch_name_from_vendor`` and email a summary to ``notify_email``.

    Writes the missing-data report and the full LIMS pull to CSV files under ``/results`` (or the
    current directory in local runs), then lists those file paths in the email body. SES sends
    plain-text email only. Does nothing when ``notify_email`` is empty.
    """
    if not notify_email:
        logger.info(
            "Skipping audit email for %s: no notify email provided.", batch_name_from_vendor
        )
        return

    lims_data, report, modality = run_audit(batch_name_from_vendor)

    report_path = os.path.join(
        OUTPUT_DIR, f"{batch_name_from_vendor}_{modality}_missing_data.csv"
    )
    lims_path = os.path.join(OUTPUT_DIR, f"{batch_name_from_vendor}_lims_pull.csv")
    report.to_csv(report_path, index=False)
    lims_data.to_csv(lims_path, index=False)

    subject = f"{modality} Audit Report for {batch_name_from_vendor}"

    has_age_unknown = not report.empty and "age" in report.columns and (report["age"] == "UNKNOWN").any()

    if report.empty or has_age_unknown:
        audit_message = f"No missing {modality} data found."
        if has_age_unknown:
            audit_message += "\nNote: age contains a literal 'unknown' value."
    else:
        audit_message = f"Missing {modality} data table generated for {batch_name_from_vendor}"
        logger.warning(
            f"Missing {modality} data found. Please wait till corrected before proceeding with next steps."
        )
    body = "\n".join(
        [
            f"LIMS Audit for Batch: {batch_name_from_vendor}",
            f"Modality: {modality}",
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"Audit Status: {audit_message}",
            "",
            "Attached:",
            f"  - {os.path.basename(report_path)}",
            f"  - {os.path.basename(lims_path)}",
            "",
            "This is an automated notification from the OCS Submission Capsule",
        ]
    )

    message_id = send_email(
        to_address=notify_email,
        subject=subject,
        body=body,
    )
    logger.info("Email sent via SES. Message ID: %s", message_id)
