"""Build FASTQ record dataframes from exports or OCS queries and log stage summaries."""

from __future__ import annotations

import logging
from collections import Counter

import pandas as pd

from . import running_jobs_db
from .ocs_cli import get_latest_results, get_status, query_metadata

logger = logging.getLogger(__name__)

FASTQ_RECORD_COLUMNS = [
    "fastq_name",
    "study_set",
    "load_name",
    "library_prep_method_name",
    "organism_common_name",
    "batch_name_from_vendor",
    "ingest_status",
    "alignment_status",
    "post_alignment_status",
]


def load_fastq_records_df_from_exporter(exporter_path: str) -> pd.DataFrame:
    """
    Load FASTQ records from an OCS Tracker exporter CSV.

    The ocs tracker exports already has all the fields the rest of the pipeline expects, so this helper
    mostly renames columns to match ``FASTQ_RECORD_COLUMNS``. When the exporter is missing
    ``Batch Name From Vendor``, the value is filled in by looking up each FASTQ on OCS.

    Parameters
    ----------
    exporter_path
        Path to the exporter CSV file.

    Return
    ----------
    pd.DataFrame
        Dataframe with one row per FASTQ and standardized column names.

    """
    fastq_records_df = pd.read_csv(exporter_path).dropna(how="all")
    fastq_records_df = fastq_records_df.replace(", ", "; ")

    if "Batch Name From Vendor" not in fastq_records_df.columns:
        batch_name_from_vendor_list = [
            query_metadata(fastq_name=fastq_name).iloc[0]["batch_name_from_vendor"]
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
        "Alignment": "alignment_status",
        "Post Alignment": "post_alignment_status",
    }
    fastq_records_df = fastq_records_df[list(exporter_column_mapping)].rename(
        columns=exporter_column_mapping
    )

    for index, fastq_record in fastq_records_df.iterrows():
        for status_col, job_type in [("alignment_status", "alignment"), ("post_alignment_status", "postqc")]:
            if pd.isna(fastq_record[status_col]):
                fastq_records_df.at[index, status_col] = running_jobs_db.check_job_status(
                    fastq_name=fastq_record["fastq_name"], job_type=job_type
                )

    return fastq_records_df


def load_fastq_records_df_from_batch(batch_name_from_vendor: str) -> pd.DataFrame:
    """
    Build a FASTQ records dataframe for every sample in a vendor batch.

    Metadata for the whole batch is fetched in one query, and ``check_all_fastq_stage_status``
    then fills in the ingest, alignment, and post-alignment statuses.

    Parameters
    ----------
    batch_name_from_vendor
        Batch name from vendor to query from OCS.

    Return
    ----------
    pd.DataFrame
        Dataframe containing FASTQ metadata and stage status columns for the batch.

    """
    fastq_records_df = query_metadata(batch_name_from_vendor=batch_name_from_vendor)
    fastq_records_df = check_all_fastq_stage_status(fastq_records_df=fastq_records_df)
    return fastq_records_df[FASTQ_RECORD_COLUMNS]


def load_fastq_records_df_from_fastq_names(fastq_names: list[str]) -> pd.DataFrame:
    """
    Build a FASTQ records dataframe from an explicit list of FASTQ names.

    Each FASTQ is queried individually for metadata, the stage statuses are added, and the
    per-FASTQ frames are concatenated into one result.

    Parameters
    ----------
    fastq_names
        FASTQ names to include in the output dataframe.

    Return
    ----------
    pd.DataFrame
        Dataframe containing one row per requested FASTQ.

    """
    fastq_record_df_list = list()
    for fastq_name in fastq_names:
        fastq_metadata_df = query_metadata(fastq_name=fastq_name)
        fastq_record_df_list.append(
            check_all_fastq_stage_status(fastq_records_df=fastq_metadata_df)
        )

    if not fastq_record_df_list:
        return pd.DataFrame(columns=FASTQ_RECORD_COLUMNS)

    return pd.concat(fastq_record_df_list)[FASTQ_RECORD_COLUMNS]


def check_all_fastq_stage_status(fastq_records_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ingest, alignment, and post-alignment status columns to FASTQ records.

    When every row shares a single ``batch_name_from_vendor``, the latest result entries are
    pulled once per stage for the whole batch and matched back to each FASTQ; otherwise each
    FASTQ is queried on its own. If OCS has no result yet for alignment or post-alignment, the
    function falls back to ``running_jobs_db`` to see if an in-flight job was started from this
    capsule.

    Parameters
    ----------
    fastq_records_df
        DataFrame containing FASTQ metadata. Must include at least ``fastq_name`` and
        ``batch_name_from_vendor``.

    Return
    ----------
    pd.DataFrame
        Input dataframe with ``ingest_status``, ``alignment_status``, and
        ``post_alignment_status`` filled in.

    """
    unique_batch_names_from_vendor = fastq_records_df["batch_name_from_vendor"].dropna().unique()
    entries_by_stage = {"ingest": {}, "align": {}, "post-align": {}}

    if len(unique_batch_names_from_vendor) == 1:
        batch_name_from_vendor = unique_batch_names_from_vendor[0]
        for stage in entries_by_stage:
            entries_by_stage[stage] = get_latest_results(stage, batch_name_from_vendor=batch_name_from_vendor)

    for index, fastq_record in fastq_records_df.iterrows():
        fastq_name = fastq_record["fastq_name"]
        logger.info("Checking Status for %s", fastq_name)

        latest_ingest_entry = entries_by_stage["ingest"].get(fastq_name) or get_latest_results(
            "ingest", fastq_name=fastq_name
        )
        if latest_ingest_entry:
            ingest_status = get_status(latest_entry=latest_ingest_entry, status_type="ingest")
            fastq_records_df.at[index, "ingest_status"] = ingest_status
            logger.info("  - Ingest Status: %s", ingest_status)
        else:
            fastq_records_df.at[index, "ingest_status"] = "NOT COMPLETED"
            logger.info("  - No ingest entry found for %s", fastq_name)
            logger.info("  - Ingest Status: NOT COMPLETED")

        for stage, label, status_col, job_type in [
            ("align", "Alignment", "alignment_status", "alignment"),
            ("post-align", "Post-Alignment", "post_alignment_status", "postqc"),
        ]:
            latest_entry = entries_by_stage[stage].get(fastq_name) or get_latest_results(
                stage, fastq_name=fastq_name
            )
            if latest_entry:
                status = get_status(latest_entry=latest_entry, status_type=stage)
                fastq_records_df.at[index, status_col] = status
                logger.info("  - %s Status: %s", label, status)
            else:
                db_status = running_jobs_db.check_job_status(fastq_name=fastq_name, job_type=job_type)
                status = db_status or "NOT COMPLETED"
                fastq_records_df.at[index, status_col] = status
                logger.info("  - %s Status: %s", label, status)

    return fastq_records_df


def generate_status_summary(
    stage: str,
    fastq_samples: list,
    status_field: str,
) -> str:
    """
    Build a one-line summary of how many FASTQs are in each status for a stage.

    ``NOT COMPLETED`` is dropped so the log line only highlights active or completed states.

    Parameters
    ----------
    stage
        Label to show at the start of the summary line, such as ``Ingest`` or ``Alignment``.
    fastq_samples
        List of dict-like FASTQ records. Each record must support ``.get(status_field, ...)``.
    status_field
        Status field to count, such as ``ingest_status`` or ``align_status``.

    Return
    ----------
    str
        Single formatted summary line suitable for ``logger.info``.

    """
    status_counts = Counter(
        sample.get(status_field, "NOT COMPLETED") for sample in fastq_samples
    )
    total_samples = len(fastq_samples)

    summary_part_list = [
        f"{status.replace('_', ' ').title()} {count}/{total_samples}"
        for status, count in status_counts.items()
        if status != "NOT COMPLETED"
    ] or [f"Completed 0/{total_samples}"]

    return f"  {stage}: {' '.join(summary_part_list)}"


def log_fastq_status_summaries(
    fastq_records_df: pd.DataFrame, from_tracker_exporter: bool
) -> None:
    """
    Log one-line status summaries for ingest, alignment, and post-alignment.

    Only runs for live OCS lookups; exporter input is skipped because those statuses are already
    summarized in the CSV itself.

    Parameters
    ----------
    fastq_records_df
        FASTQ records dataframe containing stage status columns.
    from_tracker_exporter
        ``True`` when records came from an exporter CSV instead of live OCS queries.

    Return
    ----------
    None

    """
    if from_tracker_exporter or fastq_records_df.empty:
        return

    fastq_records = fastq_records_df.to_dict("records")

    logger.info("Status Summary:")
    logger.info(
        generate_status_summary(
            stage="Ingest",
            fastq_samples=fastq_records,
            status_field="ingest_status",
        )
    )
    logger.info(
        generate_status_summary(
            stage="Alignment",
            fastq_samples=fastq_records,
            status_field="alignment_status",
        )
    )
    logger.info(
        generate_status_summary(
            stage="Post-Alignment",
            fastq_samples=fastq_records,
            status_field="post_alignment_status",
        )
    )
