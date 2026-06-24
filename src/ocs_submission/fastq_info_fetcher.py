"""Build FASTQ record dataframes from exports or OCS queries and log stage summaries."""

from __future__ import annotations

import logging
from collections import Counter

import pandas as pd

from . import running_jobs_db
from .ocs_cli import get_latest_results, query_metadata
from .stages import Stage

logger = logging.getLogger(__name__)

FASTQ_RECORD_COLUMNS = [
    "fastq_name",
    "study_set",
    "load_name",
    "library_prep_method_name",
    "organism_common_name",
    "batch_name_from_vendor",
    "ingest_status",
    "align_status",
    "postalign_status",
]


def load_fastq_records_df_from_exporter(exporter_path: str) -> pd.DataFrame:
    """Load FASTQ records from the export file from ocs tracker

    The OCS tracker export already has all the fields the rest of the pipeline expects,
    so this helper mostly renames the CSV's columns to match ``FASTQ_RECORD_COLUMNS``.
    When the export is missing ``Batch Name From Vendor``, that value is looked up on
    OCS. Empty alignment and post-alignment statuses are filled from the running jobs
    database, which tracks jobs submitted by this submission script.
    """
    fastq_records_df = pd.read_csv(exporter_path).dropna(how="all")
    fastq_records_df = fastq_records_df.replace(", ", "; ")

    if "Batch Name From Vendor" not in fastq_records_df.columns:
        metadata_df = query_metadata(
            fastq_name_list=fastq_records_df["Fastq Name"].tolist()
        )
        batch_name_from_vendor_list = [
            metadata_df.loc[fastq_name, "batch_name_from_vendor"]
            for fastq_name in fastq_records_df["Fastq Name"]
        ]
        fastq_records_df["Batch Name From Vendor"] = batch_name_from_vendor_list

    exporter_column_mapping = {
        "Fastq Name": "fastq_name",
        "Study Set": "study_set",
        "Load Name": "load_name",
        "Library Prep Method": "library_prep_method_name",
        "Organism": "organism_common_name",
        "Batch Name From Vendor": "batch_name_from_vendor",
        "Ingest": "ingest_status",
        "Alignment": "align_status",
        "Post Alignment": "postalign_status",
    }
    fastq_records_df = fastq_records_df[list(exporter_column_mapping)].rename(
        columns=exporter_column_mapping
    )

    for index, fastq_record in fastq_records_df.iterrows():
        for stage in (Stage.ALIGNMENT, Stage.POST_ALIGNMENT):
            if pd.isna(fastq_record[stage.fastq_status_column]):
                fastq_records_df.at[index, stage.fastq_status_column] = (
                    running_jobs_db.check_job_status(
                        fastq_name=fastq_record["fastq_name"],
                        stage=stage,
                    )
                )

    return fastq_records_df


def load_fastq_records_df_from_batch(batch_name_from_vendor: str) -> pd.DataFrame:
    """
    Build a dataframe for every sample in a vendor batch.

    The dataframe has the columns in ``FASTQ_RECORD_COLUMNS`` and includes batch
    metadata plus ingest, alignment, and post-alignment statuses.

    Metadata for the whole batch is fetched in one ``query_metadata`` call. Then
    ``check_all_fastq_stage_status`` fills in the ingest, alignment, and
    post-alignment statuses.
    """
    fastq_records_df = query_metadata(batch_name_from_vendor=batch_name_from_vendor)
    fastq_records_df = check_all_fastq_stage_status(fastq_records_df=fastq_records_df)
    
    return fastq_records_df[FASTQ_RECORD_COLUMNS]


def load_fastq_records_df_from_fastq_names(fastq_names: list[str]) -> pd.DataFrame:
    """
    Build a dataframe for every FASTQ name provided by the user.

    The dataframe has the columns in ``FASTQ_RECORD_COLUMNS`` and includes FASTQ
    metadata plus ingest, alignment, and post-alignment statuses.

    Metadata is fetched with ``query_metadata``. Then ``check_all_fastq_stage_status``
    fills in the ingest, alignment, and post-alignment statuses.
    """
    fastq_metadata_df = query_metadata(fastq_name_list=fastq_names)
    fastq_record_df = check_all_fastq_stage_status(fastq_records_df=fastq_metadata_df)
    return fastq_record_df[FASTQ_RECORD_COLUMNS]


def check_all_fastq_stage_status(fastq_records_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch the current status a list of fastq samples on OCS. If nothing is found:
    Fall back to the status of the fastq samples on the running jobs database. If nothing is found:
    Fall back to NOT COMPLETED.

    Parameters:
    fastq_records_df: A dataframe containing ``fastq_name`` and/or
        ``batch_name_from_vendor`` columns.

    Returns:
    The same dataframe with the status columns filled in.
    """
    unique_batch_names_from_vendor = fastq_records_df["batch_name_from_vendor"].dropna().unique()

    if len(unique_batch_names_from_vendor) == 1:
        batch_name_from_vendor = unique_batch_names_from_vendor[0]
        fastq_stage_status_df = get_latest_results(batch_name_from_vendor=batch_name_from_vendor)
    else:
        fastq_stage_status_df = get_latest_results(
            fastq_name_list=fastq_records_df["fastq_name"].tolist()
        )

    fastq_records_df = fastq_records_df.join(fastq_stage_status_df, how="left")

    for index, fastq_record in fastq_records_df.iterrows():
        fastq_name = fastq_record["fastq_name"]
        logger.info(f"Checking Status for {fastq_name}")

        ingest_status = fastq_record[Stage.INGEST.fastq_status_column]
        if ingest_status == "COMPLETED":
            logger.info(f"  - Ingest Status: {ingest_status}")
        else:
            fastq_records_df.at[index, Stage.INGEST.fastq_status_column] = "NOT COMPLETED"
            logger.info(f"  - No ingest entry found for {fastq_name}")
            logger.info("  - Ingest Status: NOT COMPLETED")

        for stage in (Stage.ALIGNMENT, Stage.POST_ALIGNMENT):
            status = fastq_record[stage.fastq_status_column]
            if status == "NOT COMPLETED":
                db_status = running_jobs_db.check_job_status(fastq_name=fastq_name, stage=stage)
                if db_status:
                    status = db_status
                fastq_records_df.at[index, stage.fastq_status_column] = status
            logger.info(f"  - {stage.ocs_stage_name} Status: {status}")

    return fastq_records_df


def log_fastq_status_summaries(
    fastq_records_df: pd.DataFrame,
) -> None:
    """
    Logs one-line status summaries for ingest, alignment, and post-alignment,

    Parameters:
    fastq_records_df: A dataframe of fastq samples and there stage status .
    """

    total_samples = len(fastq_records_df)

    logger.info("Status Summary:")
    for stage in Stage:
        status_counts = Counter(fastq_records_df[stage.fastq_status_column])
        summary_part_list = [
            f"{status.replace('_', ' ').title()} {count}/{total_samples}"
            for status, count in status_counts.items()
            if status != "NOT COMPLETED"
        ] or [f"Completed 0/{total_samples}"]

        logger.info(f"  {stage.ocs_stage_name}: {' '.join(summary_part_list)}")
