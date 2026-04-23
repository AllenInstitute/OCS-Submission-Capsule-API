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

    Pseudo code
    ----------
    read the exporter CSV and drop empty rows
    normalize cell text formatting
    if batch name is missing:
        look up the batch name for each FASTQ from OCS
    rename exporter columns to the internal FASTQ record schema
    return the selected columns
    """
    fastq_records_df = pd.read_csv(exporter_path).dropna(how="all")
    fastq_records_df = fastq_records_df.replace(", ", "; ")

    if "Batch Name From Vendor" not in fastq_records_df.columns:
        batch_names_from_vendor = [
            query_metadata(fastq_name=fastq_name).iloc[0]["batch_name_from_vendor"]
            for fastq_name in fastq_records_df["Fastq Name"]
        ]
        fastq_records_df["Batch Name From Vendor"] = batch_names_from_vendor

    exporter_column_mapping = {
        "Fastq Name": "fastq_name",
        "Study Set": "study_set",
        "Load Name": "load_name",
        "Library Prep Method Name": "library_prep_method_name",
        "Organism Common Name": "organism_common_name",
        "Batch Name From Vendor": "batch_name_from_vendor",
        "Ingest Status": "ingest_status",
        "Alignment Status": "alignment_status",
        "Post-Alignment Status": "post_alignment_status",
    }
    return fastq_records_df[list(exporter_column_mapping)].rename(
        columns=exporter_column_mapping
    )


def load_fastq_records_df_from_batch(
    batch_name_from_vendor: str, config: dict
) -> pd.DataFrame:
    """
    Build a FASTQ records dataframe for every sample in a vendor batch.

    Metadata for the whole batch is fetched in one query, and ``check_all_fastq_stage_status``
    then fills in the ingest, alignment, and post-alignment statuses.

    Parameters
    ----------
    batch_name_from_vendor
        Batch name from vendor to query from OCS.
    config
        Application configuration used by the stage status helper.

    Return
    ----------
    pd.DataFrame
        Dataframe containing FASTQ metadata and stage status columns for the batch.

    Pseudo code
    ----------
    query metadata for the batch
    add stage status columns
    return the dataframe in ``FASTQ_RECORD_COLUMNS`` order
    """
    fastq_records_df = query_metadata(batch_name_from_vendor=batch_name_from_vendor)
    fastq_records_df = check_all_fastq_stage_status(
        fastq_records_df=fastq_records_df,
        config=config,
    )
    return fastq_records_df[FASTQ_RECORD_COLUMNS].copy()


def load_fastq_records_df_from_fastq_names(
    fastq_names: list[str], config: dict
) -> pd.DataFrame:
    """
    Build a FASTQ records dataframe from an explicit list of FASTQ names.

    Each FASTQ is queried individually for metadata, the stage statuses are added, and the
    per-FASTQ frames are concatenated into one result.

    Parameters
    ----------
    fastq_names
        FASTQ names to include in the output dataframe.
    config
        Application configuration used by the stage status helper.

    Return
    ----------
    pd.DataFrame
        Dataframe containing one row per requested FASTQ.

    Pseudo code
    ----------
    create an empty list of dataframes
    for each FASTQ name:
        query metadata for that FASTQ
        add stage status columns
        append the result to the list
    concatenate the per-FASTQ dataframes and return them in standard column order
    """
    fastq_record_dfs = []
    for fastq_name in fastq_names:
        fastq_metadata_df = query_metadata(fastq_name=fastq_name)
        fastq_record_dfs.append(
            check_all_fastq_stage_status(
                fastq_records_df=fastq_metadata_df,
                config=config,
            )
        )

    if not fastq_record_dfs:
        return pd.DataFrame(columns=FASTQ_RECORD_COLUMNS)

    return pd.concat(fastq_record_dfs)[FASTQ_RECORD_COLUMNS].copy()


def check_all_fastq_stage_status(
    fastq_records_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
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
    config
        Application configuration containing status mapping groups such as
        ``alignment_complete`` and ``post_alignment_complete``.

    Return
    ----------
    pd.DataFrame
        Copy of the input dataframe with ``ingest_status``, ``alignment_status``, and
        ``post_alignment_status`` filled in.

    Pseudo code
    ----------
    copy the dataframe
    if all rows share one batch name:
        fetch latest ingest, alignment, and post-alignment results once for that batch
    for each FASTQ row:
        resolve ingest status from the latest OCS result
        resolve alignment status from OCS, or fall back to ``running_jobs_db``
        resolve post-alignment status from OCS, or fall back to ``running_jobs_db``
    return the updated dataframe
    """
    alignment_complete_statuses = config["status_mappings"]["alignment_complete"]
    post_alignment_complete_statuses = config["status_mappings"][
        "post_alignment_complete"
    ]
    fastq_records_df = fastq_records_df.copy()
    unique_batch_names_from_vendor = fastq_records_df["batch_name_from_vendor"].dropna().unique()
    ingest_entries = {}
    align_entries = {}
    post_align_entries = {}

    if len(unique_batch_names_from_vendor) == 1:
        batch_name_from_vendor = unique_batch_names_from_vendor[0]
        ingest_entries = get_latest_results("ingest", batch_name_from_vendor=batch_name_from_vendor)
        align_entries = get_latest_results("align", batch_name_from_vendor=batch_name_from_vendor)
        post_align_entries = get_latest_results("post-align", batch_name_from_vendor=batch_name_from_vendor)

    for index, fastq_record in fastq_records_df.iterrows():
        fastq_name = fastq_record["fastq_name"]
        logger.info("Checking Status for %s", fastq_name)

        latest_ingest_entry = ingest_entries.get(fastq_name) or get_latest_results(
            "ingest",
            fastq_name=fastq_name,
        )
        if latest_ingest_entry:
            ingest_status = get_status(
                latest_entry=latest_ingest_entry,
                status_type="ingest",
            )
            fastq_records_df.at[index, "ingest_status"] = ingest_status
            logger.info("  - Ingest Status: %s", ingest_status)
        else:
            fastq_records_df.at[index, "ingest_status"] = "NOT COMPLETED"
            logger.info("  - No ingest entry found for %s", fastq_name)
            logger.info("  - Ingest Status: NOT COMPLETED")

        latest_align_entry = align_entries.get(fastq_name) or get_latest_results(
            "align",
            fastq_name=fastq_name,
        )
        if latest_align_entry:
            align_status = get_status(
                latest_entry=latest_align_entry,
                status_type="align",
            )
            fastq_records_df.at[index, "alignment_status"] = align_status
            logger.info("  - Alignment Status: %s", align_status)

            if align_status in alignment_complete_statuses:
                pass
        else:
            db_align_status = running_jobs_db.check_job_status(
                fastq_name=fastq_name,
                job_type="alignment",
            )

            if db_align_status:
                fastq_records_df.at[index, "alignment_status"] = db_align_status
                logger.info("  - Alignment Status: %s", db_align_status)
            else:
                fastq_records_df.at[index, "alignment_status"] = "NOT COMPLETED"
                logger.info("  - Alignment Status: NOT COMPLETED")

        latest_post_align_entry = post_align_entries.get(fastq_name) or get_latest_results(
            "post-align",
            fastq_name=fastq_name,
        )
        if latest_post_align_entry:
            post_align_status = get_status(
                latest_entry=latest_post_align_entry,
                status_type="post-align",
            )
            fastq_records_df.at[index, "post_alignment_status"] = post_align_status
            logger.info("  - Post-Alignment Status: %s", post_align_status)

            if post_align_status in post_alignment_complete_statuses:
                pass
        else:
            db_post_align_status = running_jobs_db.check_job_status(
                fastq_name=fastq_name,
                job_type="postqc",
            )

            if db_post_align_status:
                fastq_records_df.at[index, "post_alignment_status"] = db_post_align_status
                logger.info("  - Post-Alignment Status: %s", db_post_align_status)
            else:
                fastq_records_df.at[index, "post_alignment_status"] = "NOT COMPLETED"
                logger.info("  - Post-Alignment Status: NOT COMPLETED")

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

    Pseudo code
    ----------
    count values from ``status_field``
    skip the ``NOT COMPLETED`` bucket
    format the remaining counts into one log line
    """
    status_counts = Counter(
        sample.get(status_field, "NOT COMPLETED") for sample in fastq_samples
    )
    total_samples = len(fastq_samples)

    summary_parts = [
        f"{status.replace('_', ' ').title()} {count}/{total_samples}"
        for status, count in status_counts.items()
        if status != "NOT COMPLETED"
    ] or [f"Completed 0/{total_samples}"]

    return f"  {stage}: {' '.join(summary_parts)}"


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

    Pseudo code
    ----------
    if the data came from an exporter or the dataframe is empty: return
    rename columns needed by the summary helper
    log summaries for ingest, alignment, and post-alignment
    """
    if from_tracker_exporter or fastq_records_df.empty:
        return

    fastq_records = fastq_records_df.rename(
        columns={
            "alignment_status": "align_status",
            "post_alignment_status": "post_align_status",
        }
    ).to_dict("records")

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
            status_field="align_status",
        )
    )
    logger.info(
        generate_status_summary(
            stage="Post-Alignment",
            fastq_samples=fastq_records,
            status_field="post_align_status",
        )
    )
