"""Load fastq sample metadata and stage statuses from OCS or the export file from OCS Tracker."""

import logging
from collections import Counter

import pandas as pd
from rapidfuzz import fuzz, process

from ..integrations.ocs_cli import get_latest_results, query_metadata
from ..workflow.stages import JOB_STAGES, Stage

logger = logging.getLogger(__name__)

MODALITIES = ("RTX", "MTX", "RFX")

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

EXPORTER_COLUMN_MAPPING = {
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
EXPORTER_COLUMN_ALIASES = {"Organism": ("Organism Common Name",)}


def _normalize_exporter_column_name(column_name: str) -> str:
    """Normalize capitalization and separators in a column name from the export file from OCS Tracker."""
    return " ".join(column_name.replace("_", " ").replace("-", " ").casefold().split())


def _resolve_exporter_columns(column_names: list[str]) -> dict[str, str]:
    """Match the expected column names in the export file from OCS Tracker.

    Accept capitalization differences, separators, aliases, and clear minor
    typos. Report missing or ambiguous columns rather than choosing a match.
    ``Batch Name From Vendor`` can be omitted because it is looked up on OCS.
    """
    resolved_columns: dict[str, str] = {}
    used_column_names: set[str] = set()

    for expected_column_name in EXPORTER_COLUMN_MAPPING:
        available_column_names = [column_name for column_name in column_names if column_name not in used_column_names]
        accepted_column_names = (expected_column_name, *EXPORTER_COLUMN_ALIASES.get(expected_column_name, ()))
        normalized_accepted_names = {_normalize_exporter_column_name(name) for name in accepted_column_names}
        exact_matches = [
            column_name
            for column_name in available_column_names
            if _normalize_exporter_column_name(column_name) in normalized_accepted_names
        ]

        if len(exact_matches) == 1:
            resolved_columns[expected_column_name] = exact_matches[0]
            used_column_names.add(exact_matches[0])
            continue
        if len(exact_matches) > 1:
            raise ValueError(f"Multiple CSV columns match {expected_column_name!r}: {exact_matches}")

        matches = process.extract(
            expected_column_name,
            available_column_names,
            scorer=fuzz.ratio,
            processor=_normalize_exporter_column_name,
            score_cutoff=85,
            limit=2,
        )
        if not matches:
            if expected_column_name == "Batch Name From Vendor":
                continue
            raise ValueError(
                f"CSV is missing required column {expected_column_name!r}. "
                f"Expected columns: {', '.join(EXPORTER_COLUMN_MAPPING)}"
            )

        best_match, best_score, _ = matches[0]
        if len(matches) > 1 and best_score - matches[1][1] < 5:
            raise ValueError(
                f"CSV column for {expected_column_name!r} is ambiguous. "
                f"Possible matches: {matches[0][0]!r}, {matches[1][0]!r}"
            )

        resolved_columns[expected_column_name] = best_match
        used_column_names.add(best_match)

    return resolved_columns


def load_fastq_records_df_from_exporter(exporter_path: str) -> pd.DataFrame:
    """Load fastq samples from the export file from OCS Tracker.

    The export file from OCS Tracker contains the fields the rest of the pipeline
    expects, so this helper renames the CSV's columns to match
    ``FASTQ_RECORD_COLUMNS``. Column names can differ in capitalization or
    separators, and clear minor typos are accepted.

    When the export file is missing ``Batch Name From Vendor``, that value is
    looked up on OCS for each fastq sample. Empty alignment and post-alignment
    statuses are treated as incomplete.
    """
    fastq_records_df = pd.read_csv(exporter_path).dropna(how="all")
    fastq_records_df = fastq_records_df.replace(", ", "; ")

    resolved_exporter_columns = _resolve_exporter_columns(list(fastq_records_df.columns))
    if "Batch Name From Vendor" not in resolved_exporter_columns:
        fastq_name_column = resolved_exporter_columns["Fastq Name"]
        metadata_df = query_metadata(fastq_name_list=fastq_records_df[fastq_name_column].tolist())
        fastq_records_df["Batch Name From Vendor"] = [
            metadata_df.loc[fastq_name, "batch_name_from_vendor"] for fastq_name in fastq_records_df[fastq_name_column]
        ]
        resolved_exporter_columns["Batch Name From Vendor"] = "Batch Name From Vendor"

    fastq_records_df = fastq_records_df[
        [resolved_exporter_columns[column_name] for column_name in EXPORTER_COLUMN_MAPPING]
    ].rename(
        columns={
            resolved_exporter_columns[column_name]: output_column_name
            for column_name, output_column_name in EXPORTER_COLUMN_MAPPING.items()
        }
    )
    for stage in JOB_STAGES:
        status_column = stage.fastq_status_column
        fastq_records_df[status_column] = fastq_records_df[status_column].fillna("NOT COMPLETED")

    return fastq_records_df


def infer_modality(fastq_records_df: pd.DataFrame) -> str:
    """Infer one modality from the batch name from vendor for each fastq sample.

    ATX and MTX fastq samples both use the MTX workflow. Missing or unrecognized
    batch names from vendor and mixed modalities stop the run before submission.
    """
    batch_names = fastq_records_df["batch_name_from_vendor"].astype("string")
    modalities = batch_names.str.split("-", n=1).str[0].replace({"ATX": "MTX"})
    if not modalities.isin(MODALITIES).all():
        raise ValueError(
            "Cannot infer modality: every fastq sample must have a batch name from vendor "
            "starting with RTX, MTX, RFX, or ATX."
        )
    if modalities.nunique() != 1:
        raise ValueError("Backlog runs require one modality. Submit each modality in a separate run.")
    return modalities.iloc[0]


def load_fastq_records_df_from_batch(batch_name_from_vendor: str) -> pd.DataFrame:
    """Load metadata and current stage statuses for fastq samples using the batch name from vendor."""
    fastq_records_df = query_metadata(batch_name_from_vendor=batch_name_from_vendor)
    fastq_records_df = check_all_fastq_stage_status(fastq_records_df=fastq_records_df)

    return fastq_records_df[FASTQ_RECORD_COLUMNS]


def load_fastq_records_df_from_fastq_names(fastq_names: list[str]) -> pd.DataFrame:
    """Load metadata and current stage statuses for the requested fastq samples."""
    fastq_metadata_df = query_metadata(fastq_name_list=fastq_names)
    fastq_record_df = check_all_fastq_stage_status(fastq_records_df=fastq_metadata_df)
    return fastq_record_df[FASTQ_RECORD_COLUMNS]


def load_fastq_records_df_from_load_names(load_names: list[str], modality: str | None = None) -> pd.DataFrame:
    """Load all fastq samples associated with the requested load names.

    For each load, check status using the fastq sample whose batch name from
    vendor matches the modality. If none matches, use the first fastq sample.
    Apply those statuses to all fastq samples in the load. When modality is not
    provided, infer it from the batch names from vendor.
    """
    load_metadata_df = query_metadata(load_name_list=load_names)
    modality = modality or infer_modality(load_metadata_df)
    status_record_indexes = []
    for _, load_group in load_metadata_df.groupby("load_name", sort=False):
        modality_records = load_group[load_group["batch_name_from_vendor"].str.startswith(modality, na=False)]
        if modality_records.empty:
            modality_records = load_group
        status_record_indexes.append(modality_records.index[0])

    status_records_df = check_all_fastq_stage_status(fastq_records_df=load_metadata_df.loc[status_record_indexes])
    status_by_load_name = status_records_df.set_index("load_name")
    for stage in Stage:
        load_metadata_df[stage.fastq_status_column] = load_metadata_df["load_name"].map(
            status_by_load_name[stage.fastq_status_column]
        )

    return load_metadata_df[FASTQ_RECORD_COLUMNS]


def check_all_fastq_stage_status(fastq_records_df: pd.DataFrame) -> pd.DataFrame:
    """Check the current OCS stage statuses for each fastq sample.

    Look up statuses using the batch name from vendor when all fastq samples
    share one, otherwise use their fastq sample names. Add the statuses to the
    metadata and treat ingest as incomplete unless OCS reports it as completed.
    """
    unique_batch_names_from_vendor = fastq_records_df["batch_name_from_vendor"].dropna().unique()

    if len(unique_batch_names_from_vendor) == 1:
        batch_name_from_vendor = unique_batch_names_from_vendor[0]
        fastq_stage_status_df = get_latest_results(batch_name_from_vendor=batch_name_from_vendor)
    else:
        fastq_stage_status_df = get_latest_results(fastq_name_list=fastq_records_df["fastq_name"].tolist())

    fastq_records_df = fastq_records_df.join(fastq_stage_status_df, how="left")

    for index, fastq_record in fastq_records_df.iterrows():
        fastq_name = fastq_record["fastq_name"]
        logger.info(f"Checking Status for {fastq_name}")

        for stage in Stage:
            status = fastq_record[stage.fastq_status_column]
            stage_label = stage.ocs_stage_name.title()

            if stage == Stage.INGEST:
                if status == "COMPLETED":
                    logger.info(f"  - {stage_label} Status: {status}")
                else:
                    fastq_records_df.at[index, stage.fastq_status_column] = "NOT COMPLETED"
                    logger.info(f"  - No ingest entry found for {fastq_name}")
                    logger.info(f"  - {stage_label} Status: NOT COMPLETED")
            else:
                logger.info(f"  - {stage_label} Status: {status}")

    return fastq_records_df


def log_fastq_status_summaries(fastq_records_df: pd.DataFrame) -> None:
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
