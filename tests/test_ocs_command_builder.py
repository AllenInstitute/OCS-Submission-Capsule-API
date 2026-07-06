from __future__ import annotations

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ocs_submission.ocs_command_builder import (
    COMMAND_RECORD_COLUMNS,
    build_alignment_job_command_record,
    build_ocs_command_args,
    build_ocs_job_submission_command,
    build_post_alignment_job_command_record,
    select_command_config,
)
from ocs_submission.stages import Stage

EMAIL = "BICore@alleninstitute.org"
EXPECTED_ALIGNMENT_COMMAND_ARGS = [
    "ocs",
    "fastqs",
    "align",
    "tenx-arc",
    "--reference-names",
    "mouse_mtx_ref",
    "--load-names",
    "LOAD_1",
    "--notify",
    EMAIL,
]
EXPECTED_POSTALIGN_COMMAND_ARGS = [
    "ocs",
    "fastqs",
    "postalign",
    "tenx-arc",
    "--asset-name",
    "10x_multiome_qc",
    "--load-names",
    "LOAD_1",
]


def _command_config(
    name: str,
    library_preps: list[str],
    organisms: list[str] | None = None,
    command: list[str] | None = None,
    arguments: list[dict] | None = None,
    spacing: int = 1,
) -> dict:
    match = {"library_preps": library_preps}
    if organisms is not None:
        match["organisms"] = organisms

    return {
        "name": name,
        "match": match,
        "command": command or ["ocs"],
        "arguments": arguments or [],
        "spacing": spacing,
    }


def _expected_manifest_row(
    fastq_name: str = "NY-MX22068-2",
    align_should_execute: bool = False,
    align_command_args: list[str] | None = None,
    align_spacing: int | None = None,
    postalign_should_execute: bool = False,
    postalign_command_args: list[str] | None = None,
    postalign_spacing: int | None = None,
) -> dict:
    return {
        "fastq_name": fastq_name,
        "study_set": "StudyA",
        "load_name": "LOAD_1",
        "library_prep_method_name": "10xRSeq_Mult",
        "organism_common_name": "mouse",
        "batch_name_from_vendor": "MTX-22068",
        "modality": "MTX",
        "ingest_status": "INGEST_COMPLETE",
        "align_status": "NOT COMPLETED",
        "postalign_status": "NOT COMPLETED",
        "force_submission": None,
        "dry_run": True,
        "notify_email": EMAIL,
        "align_should_execute": align_should_execute,
        "align_command_args": align_command_args,
        "align_command": " ".join(align_command_args) if align_command_args else None,
        "align_spacing": align_spacing,
        "align_demand_id": None,
        "align_submission_success": None,
        "align_error_message": None,
        "align_executed_at": None,
        "postalign_should_execute": postalign_should_execute,
        "postalign_command_args": postalign_command_args,
        "postalign_command": " ".join(postalign_command_args) if postalign_command_args else None,
        "postalign_spacing": postalign_spacing,
        "postalign_demand_id": None,
        "postalign_submission_success": None,
        "postalign_error_message": None,
        "postalign_executed_at": None,
    }


def _assert_job_not_scheduled(result: dict, stage_prefix: str) -> None:
    assert result[f"{stage_prefix}_command_args"] is None
    assert result[f"{stage_prefix}_command"] is None
    assert result[f"{stage_prefix}_spacing"] is None


@pytest.mark.parametrize(
    "stage, command_config_field",
    [
        pytest.param(Stage.ALIGNMENT, "alignment_command_configs", id="alignment"),
        pytest.param(Stage.POST_ALIGNMENT, "post_alignment_command_configs", id="post_alignment"),
    ],
)
def test_select_command_config_returns_first_matching_config(config, stage, command_config_field):
    config["workflows"]["MTX"][command_config_field] = [
        _command_config("first", ["10xRSeq_Mult"]),
        _command_config("second", ["10xRSeq_Mult"]),
    ]

    selected = select_command_config(
        config=config,
        modality="MTX",
        stage=stage,
        library_prep_method_name="10xRSeq_Mult",
        organism_common_name="mouse",
    )

    assert selected["name"] == "first"


def test_select_command_config_matches_any_organism_when_organisms_is_omitted(config):
    selected = select_command_config(
        config=config,
        modality="MTX",
        stage=Stage.ALIGNMENT,
        library_prep_method_name="10xRSeq_Mult",
        organism_common_name="human",
    )

    assert selected["name"] == "default"


def test_select_command_config_skips_configs_restricted_to_other_organisms(config):
    config["workflows"]["MTX"]["post_alignment_command_configs"] = [
        _command_config("human", ["10xRSeq_Mult"], organisms=["human"]),
        _command_config("mouse", ["10xRSeq_Mult"], organisms=["mouse"]),
    ]

    selected = select_command_config(
        config=config,
        modality="MTX",
        stage=Stage.POST_ALIGNMENT,
        library_prep_method_name="10xRSeq_Mult",
        organism_common_name="mouse",
    )

    assert selected["name"] == "mouse"


@pytest.mark.parametrize(
    "stage, expected_error",
    [
        pytest.param(Stage.ALIGNMENT, "No MTX alignment command config found", id="alignment"),
        pytest.param(Stage.POST_ALIGNMENT, "No MTX post-alignment command config found", id="post_alignment"),
    ],
)
def test_select_command_config_rejects_unmatched_library_prep(config, stage, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        select_command_config(
            config=config,
            modality="MTX",
            stage=stage,
            library_prep_method_name="UNSUPPORTED_PREP",
            organism_common_name="mouse",
        )


@pytest.mark.parametrize("missing_field", ["match", "library_preps"])
def test_select_command_config_reports_missing_library_preps_config(config, missing_field):
    command_config = config["workflows"]["MTX"]["alignment_command_configs"][0]
    if missing_field == "match":
        del command_config["match"]
    else:
        del command_config["match"]["library_preps"]

    with pytest.raises(KeyError, match="library_preps not listed in the config file"):
        select_command_config(
            config=config,
            modality="MTX",
            stage=Stage.ALIGNMENT,
            library_prep_method_name="10xRSeq_Mult",
            organism_common_name="mouse",
        )


def test_select_post_alignment_config_rejects_unmatched_organism(config):
    config["workflows"]["MTX"]["post_alignment_command_configs"] = [
        _command_config("mouse", ["10xRSeq_Mult"], organisms=["mouse"])
    ]

    with pytest.raises(ValueError, match="No MTX post-alignment command config found"):
        select_command_config(
            config=config,
            modality="MTX",
            stage=Stage.POST_ALIGNMENT,
            library_prep_method_name="10xRSeq_Mult",
            organism_common_name="rat",
        )


def test_build_ocs_command_args_renders_template_values(config, make_fastq_record):
    template = {
        **config["workflows"]["MTX"]["alignment_command_configs"][0],
        "arguments": config["workflows"]["MTX"]["alignment_command_configs"][0]["arguments"]
        + [{"flag": "--addopts", "value": "--chemistry {chemistry}"}],
    }
    record = make_fastq_record(load_name="LOAD_1", library_prep_method_name="10xRSeq_Mult")

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert command_args == [*EXPECTED_ALIGNMENT_COMMAND_ARGS, "--addopts", "--chemistry ARC-v1"]
    assert spacing == 180


def test_build_ocs_command_args_renders_probe_set_execution_vcpus_and_valueless_flags(config, make_fastq_record):
    template = _command_config(
        name="cellflex",
        library_preps=["10xV4_FX16"],
        command=["ocs"],
        arguments=[
            {"flag": "--probe-set", "value": "{probe_set}"},
            {"flag": "--execution-vcpus", "value": "{execution_vcpus}"},
            {"flag": "--no-value"},
        ],
        spacing=60,
    )
    template["execution_vcpus"] = 180
    record = make_fastq_record(library_prep_method_name="10xV4_FX16")

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert command_args == [
        "ocs",
        "--probe-set",
        "mouse_probe_set",
        "--execution-vcpus",
        "180",
        "--no-value",
    ]
    assert spacing == 60


def test_build_ocs_command_args_uses_all_reference_fallback(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="human")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    command_args, _ = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert "human_all_ref" in command_args


def test_build_ocs_command_args_requires_matching_reference(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="mouse")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    with pytest.raises(KeyError, match="No reference for organism 'mouse'"):
        build_ocs_command_args(
            config=config,
            fastq_record=record,
            modality="RFX",
            email=EMAIL,
            command_template=template,
        )


def test_build_ocs_command_args_requires_known_organism_reference(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="rat")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    with pytest.raises(KeyError, match="rat"):
        build_ocs_command_args(
            config=config,
            fastq_record=record,
            modality="MTX",
            email=EMAIL,
            command_template=template,
        )


@pytest.mark.parametrize(
    "ingest_status, align_status, force_submission, should_execute",
    [
        pytest.param("INGEST_COMPLETE", "NOT COMPLETED", None, True, id="ingested-and-not-aligned"),
        pytest.param("NOT COMPLETED", "NOT COMPLETED", None, False, id="ingest-not-complete"),
        pytest.param("INGEST_COMPLETE", "COMPLETED", None, False, id="alignment-complete"),
        pytest.param("INGEST_COMPLETE", "IN_PROGRESS", None, False, id="alignment-in-progress"),
        pytest.param("INGEST_COMPLETE", "COMPLETED", "alignment", True, id="force-alignment"),
        pytest.param("NOT COMPLETED", "NOT COMPLETED", "alignment", False, id="force-does-not-bypass-ingest"),
    ],
)
def test_alignment_submission_decision(
    config,
    make_fastq_record,
    ingest_status,
    align_status,
    force_submission,
    should_execute,
):
    record = make_fastq_record(ingest_status=ingest_status, align_status=align_status)

    result = build_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=force_submission,
    )

    assert result["align_should_execute"] is should_execute
    if should_execute:
        assert result["align_command_args"] == EXPECTED_ALIGNMENT_COMMAND_ARGS
        assert result["align_command"] == " ".join(EXPECTED_ALIGNMENT_COMMAND_ARGS)
        assert result["align_spacing"] == 180
    else:
        _assert_job_not_scheduled(result, "align")


@pytest.mark.parametrize(
    "align_status, postalign_status, alignment_should_execute, force_submission, should_execute",
    [
        pytest.param("COMPLETED", "NOT COMPLETED", False, None, True, id="aligned-and-not-postaligned"),
        pytest.param("NOT COMPLETED", "NOT COMPLETED", False, None, False, id="alignment-not-complete"),
        pytest.param("COMPLETED", "NOT COMPLETED", True, None, False, id="alignment-scheduled-this-pass"),
        pytest.param("COMPLETED", "COMPLETED", False, None, False, id="postalignment-complete"),
        pytest.param("COMPLETED", "IN_PROGRESS", False, None, False, id="postalignment-in-progress"),
        pytest.param("COMPLETED", "COMPLETED", False, "post-alignment", True, id="force-postalignment"),
    ],
)
def test_post_alignment_submission_decision(
    config,
    make_fastq_record,
    align_status,
    postalign_status,
    alignment_should_execute,
    force_submission,
    should_execute,
):
    record = make_fastq_record(
        align_status=align_status,
        postalign_status=postalign_status,
    )

    result = build_post_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=force_submission,
        alignment_should_execute=alignment_should_execute,
    )

    assert result["postalign_should_execute"] is should_execute
    if should_execute:
        assert result["postalign_command_args"] == EXPECTED_POSTALIGN_COMMAND_ARGS
        assert result["postalign_command"] == " ".join(EXPECTED_POSTALIGN_COMMAND_ARGS)
        assert result["postalign_spacing"] == 60
    else:
        _assert_job_not_scheduled(result, "postalign")


def test_post_alignment_requires_matching_library_prep(config, make_fastq_record):
    record = make_fastq_record(
        align_status="COMPLETED",
        postalign_status="NOT COMPLETED",
        library_prep_method_name="unsupported_prep",
    )

    with pytest.raises(ValueError, match="No MTX post-alignment command config found"):
        build_post_alignment_job_command_record(
            fastq_record=record,
            modality="MTX",
            config=config,
            email=EMAIL,
            force_submission=None,
            alignment_should_execute=False,
        )


@pytest.mark.parametrize(
    "align_status, postalign_status, alignment_should_execute",
    [
        pytest.param("NOT COMPLETED", "NOT COMPLETED", False, id="alignment-not-complete"),
        pytest.param("COMPLETED", "NOT COMPLETED", True, id="alignment-scheduled-this-pass"),
        pytest.param("COMPLETED", "COMPLETED", False, id="postalignment-complete"),
        pytest.param("COMPLETED", "IN_PROGRESS", False, id="postalignment-in-progress"),
    ],
)
def test_post_alignment_does_not_require_matching_library_prep_when_not_scheduled(
    config,
    make_fastq_record,
    align_status,
    postalign_status,
    alignment_should_execute,
):
    record = make_fastq_record(
        align_status=align_status,
        postalign_status=postalign_status,
        library_prep_method_name="unsupported_prep",
    )

    result = build_post_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        alignment_should_execute=alignment_should_execute,
    )

    assert result["postalign_should_execute"] is False
    _assert_job_not_scheduled(result, "postalign")


def test_build_ocs_job_submission_command_allows_alignment_without_post_alignment_config(config, make_fastq_record):
    config["workflows"]["MTX"]["alignment_command_configs"][0]["match"]["library_preps"].append("align_only_prep")
    record = make_fastq_record(library_prep_method_name="align_only_prep")

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record)]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    assert bool(result.at[0, "align_should_execute"]) is True
    assert bool(result.at[0, "postalign_should_execute"]) is False
    assert result.at[0, "align_command_args"] == EXPECTED_ALIGNMENT_COMMAND_ARGS
    assert result.at[0, "postalign_command_args"] is None


def test_expected_manifest_row_matches_command_record_schema():
    """Pin the test helper to the production manifest schema.

    ``_expected_manifest_row`` mirrors ``COMMAND_RECORD_COLUMNS`` by hand so the manifest
    assertions stay readable with explicit expected values. But ``build_ocs_job_submission_command``
    selects columns via ``columns=COMMAND_RECORD_COLUMNS``, so a column added to the source (e.g. a
    new ``JOB_RECORD_FIELDS`` entry) but missing from the helper would slip through the frame
    comparisons as an untested ``NaN``. This guard turns that silent drift into a loud failure.
    """
    expected_columns = set(COMMAND_RECORD_COLUMNS)
    helper_columns = set(_expected_manifest_row())
    assert helper_columns == expected_columns, (
        "_expected_manifest_row drifted from COMMAND_RECORD_COLUMNS. "
        f"Missing from helper: {sorted(expected_columns - helper_columns)}. "
        f"Extra in helper: {sorted(helper_columns - expected_columns)}."
    )


def test_build_ocs_job_submission_command_returns_expected_manifest_row(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="mouse")

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record)]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    expected = pd.DataFrame(
        [
            _expected_manifest_row(
                align_should_execute=True,
                align_command_args=EXPECTED_ALIGNMENT_COMMAND_ARGS,
                align_spacing=180,
            )
        ],
        columns=COMMAND_RECORD_COLUMNS,
    )

    assert_frame_equal(result, expected)


def test_build_ocs_job_submission_command_can_schedule_post_alignment(config, make_fastq_record):
    record = make_fastq_record(align_status="COMPLETED", postalign_status="NOT COMPLETED")

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record)]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    expected = pd.DataFrame(
        [
            {
                **_expected_manifest_row(
                    postalign_should_execute=True,
                    postalign_command_args=EXPECTED_POSTALIGN_COMMAND_ARGS,
                    postalign_spacing=60,
                ),
                "align_status": "COMPLETED",
            }
        ],
        columns=COMMAND_RECORD_COLUMNS,
    )

    assert_frame_equal(result, expected)


def test_build_ocs_job_submission_command_handles_mixed_rows(config, make_fastq_record):
    records = [
        make_fastq_record(fastq_name="needs-align"),
        make_fastq_record(
            fastq_name="needs-postalign",
            align_status="COMPLETED",
            postalign_status="NOT COMPLETED",
        ),
    ]

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record) for record in records]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    expected = pd.DataFrame(
        [
            _expected_manifest_row(
                fastq_name="needs-align",
                align_should_execute=True,
                align_command_args=EXPECTED_ALIGNMENT_COMMAND_ARGS,
                align_spacing=180,
            ),
            {
                **_expected_manifest_row(
                    fastq_name="needs-postalign",
                    postalign_should_execute=True,
                    postalign_command_args=EXPECTED_POSTALIGN_COMMAND_ARGS,
                    postalign_spacing=60,
                ),
                "align_status": "COMPLETED",
            },
        ],
        columns=COMMAND_RECORD_COLUMNS,
    )

    assert_frame_equal(result, expected)


def test_build_ocs_job_submission_command_returns_empty_manifest_with_schema(config):
    empty_fastq_records = pd.DataFrame(
        columns=[
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
    )

    result = build_ocs_job_submission_command(
        fastq_records_df=empty_fastq_records,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    expected = pd.DataFrame(columns=COMMAND_RECORD_COLUMNS)
    assert_frame_equal(result, expected)
