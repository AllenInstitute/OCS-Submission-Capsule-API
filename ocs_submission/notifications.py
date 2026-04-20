"""Email summaries after command execution (Code Ocean email capsule)."""

from __future__ import annotations

import logging
import os
from datetime import datetime

import pandas as pd
from codeocean import CodeOcean
from codeocean.computation import NamedRunParam, RunParams

logger = logging.getLogger(__name__)

STAGES = [("alignment", "alignment"), ("postqc", "post_alignment")]


def run_email_capsule(subject: str, body: str, recipient: str) -> None:
    """
    Run the Code Ocean email capsule and wait for it to finish.

    Reads ``CO_DOMAIN``, ``ACCESS_TOKEN``, and ``EMAIL_CAPSULE_ID`` from the environment, starts
    the capsule with ``to``, ``subject``, and ``body`` as named parameters, and polls until the
    computation completes.

    Parameters
    ----------
    subject
        Email subject line.
    body
        Plain-text email body.
    recipient
        Recipient address passed as the capsule ``to`` parameter.

    Return
    ----------
    None

    Pseudo code
    ----------
    load CO_DOMAIN, ACCESS_TOKEN, EMAIL_CAPSULE_ID from env
    RunParams(capsule_id, named_parameters=[to, subject, body])
    CodeOcean(...).computations.run_capsule; wait_until_completed
    log computation id
    """
    client = CodeOcean(
        domain=os.environ["CO_DOMAIN"],
        token=os.environ["ACCESS_TOKEN"],
    )
    run_params = RunParams(
        capsule_id=os.environ["EMAIL_CAPSULE_ID"],
        named_parameters=[
            NamedRunParam(param_name="to", value=recipient),
            NamedRunParam(param_name="subject", value=subject),
            NamedRunParam(param_name="body", value=body),
        ],
    )
    computation = client.computations.run_capsule(run_params)
    computation = client.computations.wait_until_completed(
        computation,
        polling_interval=30,
        timeout=3600,
    )
    logger.info("Email sent via Code Ocean. Computation ID: %s", computation.id)


def _stage_outcome(record, prefix: str) -> dict | None:
    """
    Read one stage's execution outcome from a command record into a flat dict.

    Returns ``None`` when the stage was not executed (submission_success is ``None``). Otherwise
    returns a dict with the identity fields and either demand id (success) or error/output
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

    Pseudo code
    ----------
    read {prefix}_submission_success
    if None: return None
    return dict(success, time, fastq_name, load_name, command, demand_id, error_message, output)
    """
    submission_success = getattr(record, f"{prefix}_submission_success")
    if submission_success is None:
        return None
    return {
        "success": bool(submission_success),
        "time": getattr(record, f"{prefix}_executed_at"),
        "fastq_name": record.fastq_name,
        "load_name": record.load_name,
        "command": getattr(record, f"{prefix}_command"),
        "demand_id": getattr(record, f"{prefix}_demand_id"),
        "error_message": getattr(record, f"{prefix}_error_message") or "Unknown error",
        "output": getattr(record, f"{prefix}_output") or "No output",
    }


def _format_block(index: int, job_type: str, outcome: dict) -> str:
    """
    Format one submission or failure block for the summary email body.

    Success blocks include the demand id; failure blocks include the error message and captured
    OCS output. Both include the common identity fields and the command that was submitted.

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

    Pseudo code
    ----------
    build the common header lines
    insert Demand ID or Error based on outcome["success"]
    append Output line on failure
    join with newlines
    """
    lines = [
        f"{index}. Fastq Name: {outcome['fastq_name']}",
        f"   Load Name: {outcome['load_name']}",
        f"   Job Type: {job_type}",
        f"   Time: {outcome['time']}",
        f"   Command: {outcome['command']}",
    ]
    if outcome["success"]:
        lines.insert(3, f"   Demand ID: {outcome['demand_id']}")
    else:
        lines.insert(3, f"   Error: {outcome['error_message']}")
        lines.append(f"   Output: {outcome['output']}")
    return "\n".join(lines) + "\n"


def send_command_summary_email(
    ocs_job_commands_df: pd.DataFrame, notify_email: str
) -> None:
    """
    Email a summary of real submissions and failures after execution.

    Does nothing if ``notify_email`` is empty or the frame is empty. Dry-run rows are skipped.
    Walks each row and each stage, routing stage outcomes into success and failure lists, then
    builds a plain-text email body and sends it via ``run_email_capsule``. If the frame carries
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

    Pseudo code
    ----------
    if no email or empty df: return
    for each non-dry-run row, for each stage:
        collect outcome; route to successes or failures
    if nothing to send: return
    derive batch_label from unique batch names (len == 1)
    build subject and body_parts
    run_email_capsule(subject, body, notify_email)
    """
    if not notify_email or ocs_job_commands_df.empty:
        return

    successes: list[tuple[str, dict]] = []
    failures: list[tuple[str, dict]] = []
    for record in ocs_job_commands_df.itertuples(index=False):
        if record.dry_run:
            continue
        for job_type, prefix in STAGES:
            outcome = _stage_outcome(record, prefix)
            if outcome is None:
                continue
            (successes if outcome["success"] else failures).append((job_type, outcome))

    if not successes and not failures:
        return

    batches = ocs_job_commands_df["batch_name_from_vendor"].dropna().unique().tolist()
    batch_label = batches[0] if len(batches) == 1 else None
    subject = (
        f"OCS Job Submission Summary - Batch: {batch_label}"
        if batch_label
        else "OCS Job Submission Summary"
    )

    body_parts = [
        "OCS Submission Summary Notification",
        "===================================",
        "",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total Jobs Processed: {len(successes) + len(failures)}",
        f"Successful Submissions: {len(successes)}",
        f"Failed Submissions: {len(failures)}",
    ]
    if batch_label:
        body_parts.append(f"Batch Name: {batch_label}")

    if successes:
        body_parts.extend(["", "Successful Submissions:", "-" * 50])
        body_parts.extend(
            _format_block(i, job_type, outcome)
            for i, (job_type, outcome) in enumerate(successes, 1)
        )

    if failures:
        body_parts.extend(["", "Failed Submissions:", "-" * 50])
        body_parts.extend(
            _format_block(i, job_type, outcome)
            for i, (job_type, outcome) in enumerate(failures, 1)
        )

    body_parts.append("")
    body_parts.append("This is an automated notification from the OCS Submission Capsule")

    run_email_capsule(subject, "\n".join(body_parts), notify_email)
