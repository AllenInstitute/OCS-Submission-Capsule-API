"""Email summaries after command execution."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import pandas as pd

from . import OUTPUT_DIR
from .audit import run_audit
from .environment import clear_aws_credential_env
from .ocs_command_builder import unconfigured_library_prep_fastq_names
from .stages import Stage

logger = logging.getLogger(__name__)

SES_REGION = "us-west-2"
SES_SOURCE = "notifications@allenneuraldynamics.org"


def send_email(
    email: str,
    subject: str,
    body: str,
    attachment_paths: list[str] | None = None,
) -> str:
    """
    Sends a plain-text email via AWS SES.

    Parameters:
    email: The recipient email address.
    subject: The subject line of the email.
    body: The plain-text body of the email.
    attachment_paths: Optional file paths to attach to the email.

    Returns:
    The SES message id of the sent email.
    """
    # Clear OCS AWS creds so boto3 uses the SES credential chain.
    clear_aws_credential_env()

    ses = boto3.client("ses", region_name=SES_REGION)
    if attachment_paths:
        message = MIMEMultipart()
        message["Subject"] = subject
        message["From"] = SES_SOURCE
        message["To"] = email
        message.attach(MIMEText(body, "plain", "utf-8"))

        for attachment_path in attachment_paths:
            with open(attachment_path, "rb") as attachment_file:
                part = MIMEApplication(attachment_file.read())
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(attachment_path),
            )
            message.attach(part)

        response = ses.send_raw_email(
            Source=SES_SOURCE,
            Destinations=[email],
            RawMessage={"Data": message.as_bytes()},
        )
        return response["MessageId"]

    response = ses.send_email(
        Source=SES_SOURCE,
        Destination={"ToAddresses": [email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body, "Charset": "UTF-8"},
            },
        },
    )
    return response["MessageId"]


def _stage_outcome(fastq_record, ocs_stage_name: str) -> dict | None:
    """
    Checks the outcome of a pipeline stage submission.

    Parameters:
    record: Row from ocs_job_commands_df with fastq_name, load_name, and stage
        submission fields (success, execution time, command, demand id, error).
    ocs_stage_name: The pipeline stage name that was submitted.

    Returns:
    None if the stage was not executed; otherwise a dict with the outcome fields.
    """
    submission_success = getattr(fastq_record, f"{ocs_stage_name}_submission_success")
    if submission_success is None:
        return None
    return {
        "success": submission_success,
        "time": getattr(fastq_record, f"{ocs_stage_name}_executed_at"),
        "fastq_name": fastq_record.fastq_name,
        "load_name": fastq_record.load_name,
        "command": getattr(fastq_record, f"{ocs_stage_name}_command"),
        "demand_id": getattr(fastq_record, f"{ocs_stage_name}_demand_id"),
        "error_message": getattr(fastq_record, f"{ocs_stage_name}_error_message"),
    }


def _format_block(index: int, job_type: str, outcome: dict) -> str:
    """
    Formats one submission or failure block for the email body.

    Parameters:
    index: The 1-based position of this entry in its section.
    job_type: The stage label shown in the email.
    outcome: A _stage_outcome dict for this submission.

    Returns:
    A multi-line block string.
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


def send_command_summary_email(ocs_job_commands_df: pd.DataFrame, notify_email: str) -> None:
    """
    Emails a summary of submissions and failures after execution.

    Parameters:
    ocs_job_commands_df: The post-execution dataframe with align and postalign columns.
    notify_email: The recipient email address; an empty value is a no-op.
    """
    # Do not send an email if no recipient was provided or there are no fastq samples to report.
    if not notify_email or ocs_job_commands_df.empty:
        return

    success_list: list[tuple[str, dict]] = list()
    failure_list: list[tuple[str, dict]] = list()
    for fastq_record in ocs_job_commands_df.itertuples(index=False):
        if fastq_record.dry_run:
            continue
        for stage in (Stage.ALIGNMENT, Stage.POST_ALIGNMENT):
            outcome = _stage_outcome(fastq_record, stage.ocs_stage_name)
            if outcome is None:
                continue
            if outcome["success"]:
                success_list.append((stage.ocs_stage_name, outcome))
            else:
                failure_list.append((stage.ocs_stage_name, outcome))

    unconfigured_fastq_names = unconfigured_library_prep_fastq_names(ocs_job_commands_df)

    # Do not send an email if there are no submission attempts and no fastq samples with unconfigured library preps.
    if not (success_list or failure_list or unconfigured_fastq_names):
        return

    batches = ocs_job_commands_df["batch_name_from_vendor"].dropna().unique()
    batch_label = batches[0] if len(batches) == 1 else None
    subject = f"OCS Job Submission Summary - Batch: {batch_label}" if batch_label else "OCS Job Submission Summary"

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
    if unconfigured_fastq_names:
        body_part_list.append(
            "The following Fastq Name have library prep names not matching the configuration file: "
            f"{', '.join(unconfigured_fastq_names)}"
        )

    if success_list:
        body_part_list.extend(["", "Successful Submissions:", "-" * 50])
        body_part_list.extend(
            _format_block(i, job_type, outcome) for i, (job_type, outcome) in enumerate(success_list, 1)
        )

    if failure_list:
        body_part_list.extend(["", "Failed Submissions:", "-" * 50])
        body_part_list.extend(
            _format_block(i, job_type, outcome) for i, (job_type, outcome) in enumerate(failure_list, 1)
        )

    body_part_list.append("")
    body_part_list.append("This is an automated notification from the OCS Submission Capsule")

    message_id = send_email(
        email=notify_email,
        subject=subject,
        body="\n".join(body_part_list),
    )
    logger.info(f"Email sent via SES. Message ID: {message_id}")


def send_audit_email(batch_name_from_vendor: str, notify_email: str) -> None:
    """
    Runs the LIMS audit for a batch and emails a summary with CSV attachments.

    Parameters:
    batch_name_from_vendor: The vendor batch name to audit.
    notify_email: The recipient email address; an empty value is a no-op.
    """
    if not notify_email:
        logger.info("Skipping audit email for %s: no notify email provided.", batch_name_from_vendor)
        return

    lims_data, report, modality = run_audit(batch_name_from_vendor)

    report_path = os.path.join(OUTPUT_DIR, f"{batch_name_from_vendor}_{modality}_missing_data.csv")
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
        logger.warning(f"Missing {modality} data found. Please wait till corrected before proceeding with next steps.")
    body = "\n".join(
        [
            f"LIMS Audit for Batch: {batch_name_from_vendor}",
            f"Modality: {modality}",
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"Audit Status: {audit_message}",
            "",
            "Attached files:",
            os.path.basename(report_path),
            os.path.basename(lims_path),
            "",
            "This is an automated notification from the OCS Submission Capsule",
        ]
    )

    message_id = send_email(
        email=notify_email,
        subject=subject,
        body=body,
        attachment_paths=[report_path, lims_path],
    )
    logger.info(f"Email sent via SES. Message ID: {message_id}")
