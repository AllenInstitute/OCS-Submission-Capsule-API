"""OCS Submission Capsule.

Loads FASTQ status, builds alignment and post-alignment commands from configuration templates,
optionally submits jobs to OCS, and sends email summaries.
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

import json
import re
from pathlib import Path

import pandas as pd

from . import running_jobs_db
from .ocs_command_builder import build_ocs_job_submission_command
from .execution import execute_ocs_submission_commands
from .fetch_fastq_ocs_records import (
    log_fastq_status_summaries,
    load_fastq_records_df_from_batch,
    load_fastq_records_df_from_exporter,
    load_fastq_records_df_from_fastq_names,
)
from .notifications import send_command_summary_email

logger = logging.getLogger(__name__)

CONFIG_PATH = str(Path(__file__).resolve().parent / "config.jsonc")
DATA_MANIFEST_PATH = ".data_manifest.json"


def write_data_manifest(
    ocs_job_commands_df: pd.DataFrame, manifest_path: str
) -> None:
    """
    Write the OCS job commands dataframe to a JSON manifest in the results folder.

    Parameters
    ----------
    ocs_job_commands_df
        Dataframe of OCS job command records, typically the return value of
        ``execute_ocs_submission_commands``.
    manifest_path
        Destination path for the manifest file. The parent directory is created if missing.

    Return
    ----------
    None

    Pseudo code
    ----------
    ensure the parent directory exists
    write ocs_job_commands_df as JSON records with indent=2
    log the path that was written
    """
    manifest_file = Path(manifest_path)
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    ocs_job_commands_df.to_json(manifest_file, orient="records", indent=2)
    logger.info("Wrote data manifest to %s", manifest_file)


def load_jsonc_config(config_path: str) -> dict:
    """
    Read a JSON-with-comments file and return a plain Python dict.

    The standard ``json`` module does not accept comments, so block comments (``/* ... */``) and
    line comments (``// ...``) are stripped out before parsing.

    Parameters
    ----------
    config_path
        Path to the JSONC configuration file on disk.

    Return
    ----------
    dict
        Parsed configuration object.

    Pseudo code
    ----------
    read file as text
    strip /* ... */ and // ... lines
    json.loads(text)
    return dict
    """
    with open(config_path, "r") as file:
        jsonc_text = file.read()

    json_text = re.sub(r"/\*.*?\*/", "", jsonc_text, flags=re.DOTALL)
    json_text = re.sub(r"^\s*//.*$", "", json_text, flags=re.MULTILINE)
    return json.loads(json_text)


def parse_args() -> argparse.Namespace:
    """
    Define and parse command-line arguments for the submission script.

    Modality is required. The run can be driven from an OCS tracker export CSV, a vendor batch
    name, or an explicit list of FASTQ names. Optional flags control forced submission, the
    notification address, and whether commands are actually executed.

    Parameters
    ----------
    (none — uses sys.argv)

    Return
    ----------
    argparse.Namespace
        Parsed arguments with attributes used by main() and loaders.

    Pseudo code
    ----------
    build ArgumentParser("OCS Submission Capsule")
    add --ocs-tracker-exporter, --modality (required), --batch-name-from-vendor,
         --fastq-names, --force-submission, --email, --dry-run, --audit
    return parser.parse_args()
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
        help="Email address for job notifications (overrides default user email)",
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
        help="Run the LIMS audit each time an alignment command is executed (true/false, default: false)",
    )
    return parser.parse_args()


def main() -> None:
    """
    Entry point for the OCS Submission Capsule workflow.

    - parse CLI arguments and reject incompatible input combinations
    - initialize the tracker database connection pool
    - load FASTQ records from the chosen source (exporter, batch, or explicit names)
    - log stage status summaries
    - build the alignment and post-alignment command records
    - submit the commands (or only log them for dry runs)
    - send a summary email when not in dry-run mode

    Parameters
    ----------
    (none)

    Return
    ----------
    None

    Pseudo code
    ----------
    args = parse_args()
    reject if batch_name_from_vendor and fastq_names both set
    dry_run = (args.dry_run == "true")
    init_connection_pool()
    config = load_jsonc_config(CONFIG_PATH)
    if ocs_tracker_exporter: load_fastq_records_df_from_exporter(...)
    elif batch_name_from_vendor: load_fastq_records_df_from_batch(...)
    elif fastq_names: load_fastq_records_df_from_fastq_names(...)
    else: raise ValueError (require one of exporter, batch, or fastq names)
    if fastq_records_df empty: log and return
    log_fastq_status_summaries(...)
    ocs_job_commands_df = build_ocs_job_submission_command(...)
    ocs_job_commands_df = execute_ocs_submission_commands(ocs_job_commands_df, job_limit)
    write_data_manifest(ocs_job_commands_df, DATA_MANIFEST_PATH)
    if not dry_run: send_command_summary_email(...)
    log completion
    """
    args = parse_args()

    if args.batch_name_from_vendor and args.fastq_names:
        raise ValueError(
            "Cannot specify both --batch-name-from-vendor and --fastq-names."
        )

    dry_run = args.dry_run == "true"
    if dry_run:
        logger.info("Dry run mode enabled. Submission commands will not be executed.")

    logger.info("Initializing database connection pool")
    running_jobs_db.init_connection_pool()

    config = load_jsonc_config(CONFIG_PATH)

    if args.ocs_tracker_exporter:
        logger.info(
            "Running OCS Submission using: %s", args.ocs_tracker_exporter
        )
        fastq_records_df = load_fastq_records_df_from_exporter(
            args.ocs_tracker_exporter
        )
    elif args.batch_name_from_vendor:
        fastq_records_df = load_fastq_records_df_from_batch(
            args.batch_name_from_vendor, config
        )
    elif args.fastq_names:
        fastq_records_df = load_fastq_records_df_from_fastq_names(
            args.fastq_names, config
        )
    else:
        raise ValueError(
            "Provide one of --ocs-tracker-exporter, --batch-name-from-vendor, or --fastq-names."
        )

    if fastq_records_df.empty:
        logger.info("No fastq metadata or workflow stage statuses found on OCS. Please manually verify this information on OCS cli.")
        return

    log_fastq_status_summaries(
        fastq_records_df=fastq_records_df,
        from_tracker_exporter=bool(args.ocs_tracker_exporter),
    )

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
        audit=args.audit == "true",
    )

    write_data_manifest(
        ocs_job_commands_df=ocs_job_commands_df,
        manifest_path=DATA_MANIFEST_PATH,
    )

    if not dry_run:
        send_command_summary_email(
            ocs_job_commands_df=ocs_job_commands_df,
            notify_email=args.email,
        )

    logger.info("OCS Submission Completed.")


if __name__ == "__main__":
    main()
