import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ocs_submission.inputs.fastq_records import (
    infer_modality,
    load_fastq_records_df_from_exporter,
    load_fastq_records_df_from_load_names,
)


@pytest.fixture
def ocs_tracker_export(fastq_records):
    return fastq_records.rename(
        columns={
            "fastq_name": "Fastq Name",
            "study_set": "Study Set",
            "load_name": "Load Name",
            "library_prep_method_name": "Library Prep Method",
            "organism_common_name": "Organism",
            "batch_name_from_vendor": "Batch Name From Vendor",
            "ingest_status": "Ingest",
            "align_status": "Alignment",
            "postalign_status": "Post Alignment",
        }
    )


def _write_ocs_tracker_export(tmp_path: Path, samples: pd.DataFrame) -> str:
    export_path = tmp_path / "ocs_tracker_export.csv"
    samples.to_csv(export_path, index=False)
    return str(export_path)


@pytest.mark.parametrize(
    "renamed_columns",
    [
        pytest.param(
            {"Fastq Name": "fastq_name", "Load Name": "load-name", "Library Prep Method": "library_prep_method"},
            id="case_and_separators",
        ),
        pytest.param({"Organism": "Organism Common Name"}, id="organism_alias"),
        pytest.param({"Fastq Name": "Fastq Nmae", "Organism": "Organisim"}, id="minor_typos"),
    ],
)
def test__load_fastq_records_df_from_exporter__accepts_header_variations(
    tmp_path, fastq_records, ocs_tracker_export, renamed_columns
):
    export_path = _write_ocs_tracker_export(tmp_path, ocs_tracker_export.rename(columns=renamed_columns))

    with patch("ocs_submission.inputs.fastq_records.query_metadata", autospec=True) as query_metadata:
        result = load_fastq_records_df_from_exporter(export_path)

    query_metadata.assert_not_called()
    assert_frame_equal(result, fastq_records)


def test__load_fastq_records_df_from_exporter__treats_empty_stage_statuses_as_incomplete(
    tmp_path, fastq_records, ocs_tracker_export
):
    ocs_tracker_export.loc[0, ["Alignment", "Post Alignment"]] = None
    export_path = _write_ocs_tracker_export(tmp_path, ocs_tracker_export)

    result = load_fastq_records_df_from_exporter(export_path)

    assert_frame_equal(result, fastq_records)


def test__load_fastq_records_df_from_exporter__looks_up_missing_batch_names_from_vendor(
    tmp_path, fastq_records, ocs_tracker_export
):
    export_path = _write_ocs_tracker_export(tmp_path, ocs_tracker_export.drop(columns="Batch Name From Vendor"))
    metadata = fastq_records.drop(columns=["ingest_status", "align_status", "postalign_status"]).set_index(
        "fastq_name", drop=False
    )

    with patch(
        "ocs_submission.inputs.fastq_records.query_metadata", autospec=True, return_value=metadata
    ) as query_metadata:
        result = load_fastq_records_df_from_exporter(export_path)

    query_metadata.assert_called_once_with(fastq_name_list=fastq_records["fastq_name"].tolist())
    assert_frame_equal(result, fastq_records)


def test__load_fastq_records_df_from_load_names__returns_fastq_samples(fastq_records):
    metadata = fastq_records.drop(columns=["ingest_status", "align_status", "postalign_status"]).set_index(
        "fastq_name", drop=False
    )
    statuses = fastq_records.set_index("fastq_name").loc[
        ["NW-MX32013-2", "NW-MX32021-10"], ["ingest_status", "align_status", "postalign_status"]
    ]
    statuses["ingest_status"] = "COMPLETED"
    statuses.loc["NW-MX32013-2", "align_status"] = "COMPLETED"

    with (
        patch(
            "ocs_submission.inputs.fastq_records.query_metadata", autospec=True, return_value=metadata
        ) as query_metadata,
        patch("ocs_submission.inputs.fastq_records.get_latest_results", autospec=True, return_value=statuses),
    ):
        result = load_fastq_records_df_from_load_names(["3492_A01", "3796_A01"], "MTX")

    query_metadata.assert_called_once_with(load_name_list=["3492_A01", "3796_A01"])
    expected = fastq_records.set_index("fastq_name", drop=False)
    expected["ingest_status"] = "COMPLETED"
    expected.loc["NW-MX32013-2", "align_status"] = "COMPLETED"
    assert_frame_equal(result, expected)


@pytest.mark.parametrize("modality", [pytest.param("MTX", id="explicit"), pytest.param(None, id="inferred")])
def test__load_fastq_records_df_from_load_names__checks_only_modality_fastq_sample(caplog, fastq_records, modality):
    caplog.set_level(logging.INFO)
    records = fastq_records[fastq_records["load_name"] == "3796_A01"]
    metadata = records.drop(columns=["ingest_status", "align_status", "postalign_status"]).set_index(
        "fastq_name", drop=False
    )
    statuses = records.set_index("fastq_name").loc[
        ["NW-MX32021-10"], ["ingest_status", "align_status", "postalign_status"]
    ]

    with (
        patch("ocs_submission.inputs.fastq_records.query_metadata", autospec=True, return_value=metadata),
        patch(
            "ocs_submission.inputs.fastq_records.get_latest_results", autospec=True, return_value=statuses
        ) as get_latest_results,
    ):
        result = load_fastq_records_df_from_load_names(["3796_A01"], modality)

    get_latest_results.assert_called_once_with(batch_name_from_vendor="MTX-32021")
    assert "Checking Status for NW-MX32021-10" in caplog.text
    assert "Checking Status for NW-AT36021-10" not in caplog.text
    assert_frame_equal(result, records.set_index("fastq_name", drop=False))


@pytest.mark.parametrize(
    "batch_names, expected",
    [
        pytest.param(["RTX-34056", "RTX-24047"], "RTX", id="rtx"),
        pytest.param(["RFX-34056"], "RFX", id="rfx"),
        pytest.param(["MTX-32021", "ATX-36021"], "MTX", id="multiome"),
    ],
)
def test__infer_modality__uses_batch_names_from_vendor(batch_names, expected):
    records = pd.DataFrame({"batch_name_from_vendor": batch_names})

    assert infer_modality(records) == expected


@pytest.mark.parametrize(
    "batch_names, message",
    [
        pytest.param(["RTX-34056", "MTX-32013"], "Backlog runs require one modality", id="mixed"),
        pytest.param(["UNKNOWN-1"], "Cannot infer modality", id="unknown"),
        pytest.param([None], "Cannot infer modality", id="missing"),
        pytest.param([float("nan")], "Cannot infer modality", id="empty_csv_batch"),
    ],
)
def test__infer_modality__rejects_missing_unknown_or_mixed_batches(batch_names, message):
    records = pd.DataFrame({"batch_name_from_vendor": batch_names})

    with pytest.raises(ValueError, match=message):
        infer_modality(records)
