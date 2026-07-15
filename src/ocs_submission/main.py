"""OCS Submission Capsule.

Loads FASTQ status, builds alignment and post-alignment commands from configuration templates,
optionally submits jobs to OCS, and sends email summaries.
"""

import argparse
import json
import logging
import os
import re
import sys

from . import OUTPUT_DIR, running_jobs_db
from .emails import send_audit_email, send_command_summary_email
from .fastq_info_fetcher import (
    load_fastq_records_df_from_batch,
    load_fastq_records_df_from_exporter,
    load_fastq_records_df_from_fastq_names,
    log_fastq_status_summaries,
)
from .ocs_cli import execute_ocs_submission_commands
from .ocs_command_builder import (
    build_ocs_job_submission_command,
    unconfigured_library_prep_fastq_names,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.jsonc")
DATA_MANIFEST_PATH = os.path.join(OUTPUT_DIR, "ocs_job_commands_manifest.json")


def load_jsonc_config(config_path: str) -> dict:
    """
    Loads a JSONC config file into a dict.

    Comments are stripped and pipe-delimited organism keys are expanded.

    Parameters:
    config_path: The path to the JSONC config file to load.

    Returns:
    A dict containing the parsed configuration with an expanded ``references`` section.
    """
    with open(config_path, "r") as file:
        jsonc_text = file.read()

    json_text = re.sub(r"/\*.*?\*/", "", jsonc_text, flags=re.DOTALL)
    json_text = re.sub(r"^\s*//.*$", "", json_text, flags=re.MULTILINE)
    config = json.loads(json_text)

    config["references"] = {
        organism.strip(): reference
        for organisms, reference in config["references"].items()
        for organism in organisms.split("|")
    }
    return config


def parse_args() -> argparse.Namespace:
    """
    Parses the submission script's command-line arguments from ``sys.argv``.

    Returns:
    An ``argparse.Namespace`` containing the parsed command-line arguments.
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
        help="One or more Fastq names (space-separated).",
    )
    parser.add_argument(
        "--force-submission",
        choices=["alignment", "post-alignment"],
        help="Force submission of alignment or post-alignment regardless of current status",
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
        help=("Run the LIMS audit each time an alignment command is executed (true/false, default: false)"),
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

    The workflow loads FASTQ records from one input source, builds and submits or
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
