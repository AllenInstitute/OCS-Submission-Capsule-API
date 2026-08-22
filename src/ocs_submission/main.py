"""Submit OCS alignment and post-alignment jobs.

Loads FASTQ status, builds alignment and post-alignment commands from configuration templates,
optionally submits jobs to OCS, and sends email summaries.
"""

import argparse
import logging
import os
import re
import sys

from . import OUTPUT_DIR
from .commands.builder import (
    build_ocs_job_submission_command,
    unconfigured_library_prep_fastq_names,
)
from .config.loader import CONFIG_PATH, load_jsonc_config
from .inputs.fastq_records import (
    load_fastq_records_df_from_batch,
    load_fastq_records_df_from_exporter,
    load_fastq_records_df_from_fastq_names,
    log_fastq_status_summaries,
)
from .integrations import running_jobs_db
from .integrations.email import send_audit_email, send_command_summary_email
from .integrations.ocs_cli import execute_ocs_submission_commands

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

DATA_MANIFEST_PATH = os.path.join(OUTPUT_DIR, "ocs_job_commands_manifest.json")


def parse_args() -> argparse.Namespace:
    """
    Return the submission script's command-line arguments.

    Returns:
    Return an ``argparse.Namespace`` with the command-line arguments.
    """
    parser = argparse.ArgumentParser(description="OCS Submission Capsule")
    parser.add_argument("--ocs-tracker-exporter", help="Export file from OCS Tracker")
    parser.add_argument(
        "--modality",
        choices=["RTX", "MTX", "RFX"],
        required=True,
        help="Modality type (RTX/MTX/RFX)",
    )
    parser.add_argument(
        "--batch-name-from-vendor",
        help="Batch name from vendor for batch information retrieval",
    )
    parser.add_argument(
        "--fastq-names",
        nargs="+",
        help="One or more FASTQ names, separated by spaces.",
    )
    parser.add_argument(
        "--force-submission",
        choices=["alignment", "post-alignment"],
        help="Submit alignment or post-alignment regardless of its current status",
    )
    parser.add_argument(
        "--email",
        "-e",
        help="Email address for OCS job notifications and run summary emails",
    )
    parser.add_argument(
        "--dry-run",
        choices=("true", "false"),
        default="false",
        help="Print commands without executing them (true/false, default: false)",
    )
    parser.add_argument(
        "--audit",
        choices=("true", "false"),
        default="false",
        help="Run the LIMS audit after each alignment command (true/false, default: false)",
    )
    parser.add_argument(
        "--batch-processing",
        choices=("true", "false"),
        default="false",
        help=(
            "Use FASTQ names instead of load names for RTX/RFX alignment and post-alignment commands "
            "(true/false, default: false)"
        ),
    )
    parser.add_argument(
        "--config",
        default=CONFIG_PATH,
        help=f"Path to a JSONC config file (default: {CONFIG_PATH})",
    )
    return parser.parse_args()


def main() -> None:
    """
    Run the OCS submission workflow.

    This workflow loads FASTQ records from one input source, builds and submits or
    dry-runs alignment and post-alignment commands, writes a JSON manifest, and sends
    summary and audit emails.
    """
    args = parse_args()

    if args.batch_name_from_vendor and args.fastq_names:
        raise ValueError("Cannot specify both --batch-name-from-vendor and --fastq-names.")

    if args.fastq_names:
        args.fastq_names = [
            fastq_name
            for raw_token in args.fastq_names
            for fastq_name in re.split(r"[,\s]+", raw_token.strip())
            if fastq_name
        ]

    dry_run = args.dry_run == "true"
    if dry_run:
        logger.info("Dry run mode enabled. Submission commands will not be executed.")

    logger.info("Initializing database connection pool")
    running_jobs_db.init_connection_pool()

    config = load_jsonc_config(args.config)

    if args.ocs_tracker_exporter:
        logger.info(f"Running OCS Submission using: {args.ocs_tracker_exporter}")
        fastq_records_df = load_fastq_records_df_from_exporter(args.ocs_tracker_exporter)
    elif args.batch_name_from_vendor:
        fastq_records_df = load_fastq_records_df_from_batch(args.batch_name_from_vendor)
    elif args.fastq_names:
        fastq_records_df = load_fastq_records_df_from_fastq_names(args.fastq_names)
    else:
        raise ValueError("Provide one of --ocs-tracker-exporter, --batch-name-from-vendor, or --fastq-names.")

    if fastq_records_df.empty:
        logger.info(
            "No fastq metadata or workflow stage statuses found on OCS. "
            "Please manually verify this information on OCS cli."
        )
        return

    log_fastq_status_summaries(fastq_records_df=fastq_records_df)

    ocs_job_commands_df = build_ocs_job_submission_command(
        fastq_records_df=fastq_records_df,
        modality=args.modality,
        config=config,
        email=args.email,
        force_submission=args.force_submission,
        dry_run=dry_run,
        batch_processing=args.batch_processing == "true",
    )

    ocs_job_commands_df = execute_ocs_submission_commands(
        ocs_job_commands_df=ocs_job_commands_df,
        job_limit=config["job_settings"]["limit"],
        poll_interval_hours=config["job_settings"].get("poll_interval_hours", 1),
    )

    ocs_job_commands_df.to_json(DATA_MANIFEST_PATH, orient="records", indent=2)
    logger.info(f"Wrote data manifest to {DATA_MANIFEST_PATH}")

    unconfigured_fastq_names = unconfigured_library_prep_fastq_names(ocs_job_commands_df)
    if unconfigured_fastq_names:
        logger.warning(
            "The following Fastq Name have library prep names not matching the configuration file: %s",
            ", ".join(unconfigured_fastq_names),
        )

    if not dry_run:
        send_command_summary_email(
            ocs_job_commands_df=ocs_job_commands_df,
            notify_email=args.email,
        )

    logger.info("OCS Submission Completed.")

    if args.audit == "true":
        for batch_name in ocs_job_commands_df["batch_name_from_vendor"].dropna().unique():
            logger.info(f"Running AUDIT for batch name from vendor: {batch_name}")
            send_audit_email(batch_name, args.email)


if __name__ == "__main__":
    main()
