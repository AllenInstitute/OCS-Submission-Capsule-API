"""Build FASTQ record dataframes from LIMS, PTS, and DATS records."""

from __future__ import annotations

import logging
from collections import Counter

import pandas as pd

from .dats_pts import DatsPtsReader
from .lims import query_lims_metadata
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
    "ingest_location",
    "align_location",
    "postalign_location",
]


def load_fastq_records_df_from_exporter(exporter_path: str) -> pd.DataFrame:
    """Load FASTQ records from an existing export and resolve source data by FASTQ name."""
    fastq_records_df = pd.read_csv(exporter_path).dropna(how="all")
    fastq_records_df = fastq_records_df.replace(", ", "; ")

    names = fastq_records_df["Fastq Name"].tolist()
    return _build_fastq_records(names)


def load_fastq_records_df_from_batch(batch_name_from_vendor: str) -> pd.DataFrame:
    """
    Build a dataframe for every sample in a vendor batch.

    The dataframe has the columns in ``FASTQ_RECORD_COLUMNS`` and includes batch
    metadata plus ingest, alignment, and post-alignment statuses.

    LIMS metadata is fetched for the batch and process status is resolved through PTS/DATS.
    """
    metadata = query_lims_metadata(batch_name_from_vendor=batch_name_from_vendor)
    return _build_fastq_records(metadata["fastq_name"].tolist(), metadata=metadata)


def load_fastq_records_df_from_fastq_names(fastq_names: list[str]) -> pd.DataFrame:
    """
    Build a dataframe for every FASTQ name provided by the user.

    The dataframe has the columns in ``FASTQ_RECORD_COLUMNS`` and includes FASTQ
    metadata plus ingest, alignment, and post-alignment statuses.

    Metadata is fetched from LIMS2 and statuses and locations from PTS/DATS.
    """
    return _build_fastq_records(fastq_names)


def _build_fastq_records(fastq_names: list[str], metadata: pd.DataFrame | None = None) -> pd.DataFrame:
    metadata = metadata if metadata is not None else query_lims_metadata(fastq_names=fastq_names)
    reader = DatsPtsReader()
    records = metadata.copy()
    stage_results = {fastq_name: reader.get_stage_results(fastq_name) for fastq_name in records["fastq_name"]}
    for stage in Stage:
        results = [stage_results[fastq_name][stage] for fastq_name in records["fastq_name"]]
        records[stage.fastq_status_column] = [result.status for result in results]
        records[f"{stage.ocs_stage_name}_location"] = [result.location for result in results]
    return records[FASTQ_RECORD_COLUMNS]


def check_all_fastq_stage_status(fastq_records_df: pd.DataFrame) -> pd.DataFrame:
    """Resolve stage status and locations directly from PTS and DATS."""
    reader = DatsPtsReader()
    for index, fastq_record in fastq_records_df.iterrows():
        for stage in Stage:
            result = reader.get_stage_result(fastq_record["fastq_name"], stage)
            fastq_records_df.at[index, stage.fastq_status_column] = result.status
            fastq_records_df.at[index, f"{stage.ocs_stage_name}_location"] = result.location
    return fastq_records_df


def log_fastq_status_summaries(
    fastq_records_df: pd.DataFrame,
) -> None:
    """
    Log one-line status summaries for ingest, alignment, and post-alignment.

    Parameters:
    fastq_records_df: A dataframe of FASTQ samples and their stage statuses.
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

        logger.info(f"  {stage.ocs_stage_name.title()}: {' '.join(summary_part_list)}")
