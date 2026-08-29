from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ocs_submission.inputs.fastq_records import load_fastq_records_df_from_exporter


def _write_exporter_csv(tmp_path: Path, columns: dict[str, list[str | None]]) -> str:
    exporter_path = tmp_path / "exporter.csv"
    pd.DataFrame(columns).to_csv(exporter_path, index=False)
    return str(exporter_path)


def test__load_fastq_records_df_from_exporter__accepts_case_and_separator_variations(tmp_path):
    exporter_path = _write_exporter_csv(
        tmp_path,
        {
            "fastq_name": ["FASTQ_1"],
            "study_set": ["StudyA"],
            "load-name": ["LOAD_1"],
            "library prep method": ["10xRSeq_Mult"],
            "organism": ["mouse"],
            "batch name from vendor": ["MTX-22068"],
            "ingest": ["INGEST_COMPLETE"],
            "alignment": [None],
            "post alignment": [None],
        },
    )

    result = load_fastq_records_df_from_exporter(exporter_path)

    assert result.loc[0, "fastq_name"] == "FASTQ_1"
    assert result.loc[0, "align_status"] == "NOT COMPLETED"
    assert result.loc[0, "postalign_status"] == "NOT COMPLETED"


def test__load_fastq_records_df_from_exporter__accepts_organism_common_name_header(tmp_path):
    exporter_path = _write_exporter_csv(
        tmp_path,
        {
            "Fastq Name": ["FASTQ_1"],
            "Study Set": ["StudyA"],
            "Load Name": ["LOAD_1"],
            "Library Prep Method": ["10xRSeq_Mult"],
            "Organism Common Name": ["mouse"],
            "Batch Name From Vendor": ["MTX-22068"],
            "Ingest": ["INGEST_COMPLETE"],
            "Alignment": ["COMPLETED"],
            "Post Alignment": ["NOT COMPLETED"],
        },
    )

    result = load_fastq_records_df_from_exporter(exporter_path)

    assert result.loc[0, "organism_common_name"] == "mouse"


def test__load_fastq_records_df_from_exporter__accepts_minor_header_typos(tmp_path):
    exporter_path = _write_exporter_csv(
        tmp_path,
        {
            "Fastq Nmae": ["FASTQ_1"],
            "Study Set": ["StudyA"],
            "Load Name": ["LOAD_1"],
            "Library Prep Method": ["10xRSeq_Mult"],
            "Organisim": ["mouse"],
            "Batch Name From Vendor": ["MTX-22068"],
            "Ingest": ["INGEST_COMPLETE"],
            "Alignment": ["COMPLETED"],
            "Post Alignment": ["NOT COMPLETED"],
        },
    )

    with patch("ocs_submission.inputs.fastq_records.query_metadata") as query_metadata:
        result = load_fastq_records_df_from_exporter(exporter_path)

    query_metadata.assert_not_called()
    assert result.loc[0, "fastq_name"] == "FASTQ_1"
    assert result.loc[0, "organism_common_name"] == "mouse"
