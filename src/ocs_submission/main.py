"""Run the OCS submission workflow for fastq samples."""

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
    MODALITIES,
    infer_modality,
    load_fastq_records_df_from_batch,
    load_fastq_records_df_from_exporter,
    load_fastq_records_df_from_fastq_names,
    load_fastq_records_df_from_load_names,
    log_fastq_status_summaries,
)
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
    """Read the parameters for the OCS submission workflow.

    Batch Processing takes precedence over Backlog or Resequencing Runs and
    requires both modality and batch name from vendor. For backlog runs, use
    the export file from OCS Tracker, load names, or fastq sample names, in that
    order. Email and dry run work with either section.
    """
    parser = argparse.ArgumentParser(description="OCS Submission Capsule")
    batch = parser.add_argument_group("Batch Processing")
    batch.add_argument(
        "--modality",
        choices=MODALITIES,
        help="Modality type (RTX/MTX/RFX), required for batch processing",
    )
    batch.add_argument(
        "--batch-name-from-vendor",
        help="Batch name from vendor to look up on OCS",
    )
    batch.add_argument(
        "--batch-processing",
        choices=("true", "false"),
        default="false",
        help=(
            "Use fastq sample names instead of load names for RTX/RFX alignment and post-alignment commands "
            "(true/false, default: false)"
        ),
    )
    batch.add_argument(
        "--force-submission",
        choices=["alignment", "post-alignment"],
        help="Submit alignment or post-alignment regardless of its current status",
    )
    batch.add_argument(
        "--audit",
        choices=("true", "false"),
        default="false",
        help="Audit each batch name from vendor after processing, except during a dry run (true/false, default: false)",
    )
    backlog = parser.add_argument_group("Backlog or Resequencing Runs")
    backlog.add_argument("--ocs-tracker-exporter", help="Export file from OCS Tracker")
    backlog.add_argument("--fastq-names", nargs="+", help="One or more fastq sample names, separated by spaces.")
    backlog.add_argument("--load-names", nargs="+", help="One or more load names, separated by spaces.")

    common = parser.add_argument_group("Common Parameters")
    common.add_argument("--email", "-e", help="Email address for OCS job notifications and run summary emails")
    common.add_argument(
        "--dry-run",
        choices=("true", "false"),
        default="false",
        help="Print commands without executing them (true/false, default: false)",
    )
    parser.add_argument(
        "--config",
        default=CONFIG_PATH,
        help=f"Path to a JSONC config file (default: {CONFIG_PATH})",
    )
    args = parser.parse_args()
    batch_selected = bool(
        args.modality
        or args.batch_name_from_vendor
        or args.force_submission
        or args.audit == "true"
        or args.batch_processing == "true"
    )
    if batch_selected:
        if args.ocs_tracker_exporter or args.fastq_names or args.load_names:
            logger.info(
                "Batch Processing selected. Ignoring the export file from OCS Tracker, "
                "fastq sample names, and load names."
            )
        args.ocs_tracker_exporter = args.fastq_names = args.load_names = None
        if not args.modality or not args.batch_name_from_vendor:
            parser.error("Batch Processing requires --modality and --batch-name-from-vendor.")
    elif args.ocs_tracker_exporter:
        args.fastq_names = args.load_names = None
    elif args.load_names:
        args.fastq_names = None
    elif not args.fastq_names:
        parser.error("Provide --ocs-tracker-exporter, --load-names, or --fastq-names for a backlog run.")
    return args


def main() -> None:
    """Run the OCS submission workflow.

    This workflow loads fastq samples from one input source, builds and submits
    or dry-runs alignment and post-alignment commands, and writes a JSON manifest.
    Summary emails are sent when there is something to report and an email
    address is provided. Audit emails are sent when audit is enabled and an
    email address is provided. Dry runs do not submit jobs or send either email.
    """
    args = parse_args()

    if args.fastq_names:
        args.fastq_names = [
            fastq_name for raw_token in args.fastq_names for fastq_name in re.split(r"[,\s]+", raw_token) if fastq_name
        ]

    if args.load_names:
        args.load_names = [
            load_name for raw_token in args.load_names for load_name in re.split(r"[,\s]+", raw_token) if load_name
        ]

    dry_run = args.dry_run == "true"
    if dry_run:
        logger.info("Dry run mode enabled. Submission commands will not be executed.")

    config = load_jsonc_config(args.config)

    if args.batch_name_from_vendor:
        fastq_records_df = load_fastq_records_df_from_batch(args.batch_name_from_vendor)
    elif args.ocs_tracker_exporter:
        logger.info(f"Running OCS Submission using: {args.ocs_tracker_exporter}")
        fastq_records_df = load_fastq_records_df_from_exporter(args.ocs_tracker_exporter)
    elif args.load_names:
        fastq_records_df = load_fastq_records_df_from_load_names(args.load_names)
    else:
        fastq_records_df = load_fastq_records_df_from_fastq_names(args.fastq_names)

    if fastq_records_df.empty:
        logger.info(
            "No fastq metadata or workflow stage statuses found on OCS. "
            "Please manually verify this information on OCS cli."
        )
        return

    modality = args.modality or infer_modality(fastq_records_df)
    status_summary_df = fastq_records_df.drop_duplicates("load_name") if args.load_names else fastq_records_df
    log_fastq_status_summaries(fastq_records_df=status_summary_df)

    ocs_job_commands_df = build_ocs_job_submission_command(
        fastq_records_df=fastq_records_df,
        modality=modality,
        config=config,
        email=args.email,
        force_submission=args.force_submission,
        dry_run=dry_run,
        batch_processing=args.batch_processing == "true",
        group_by_load_name=bool(args.load_names),
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

    if args.audit == "true" and not dry_run:
        for batch_name in ocs_job_commands_df["batch_name_from_vendor"].dropna().unique():
            logger.info(f"Running AUDIT for batch name from vendor: {batch_name}")
            send_audit_email(batch_name, args.email)


if __name__ == "__main__":
    main()
