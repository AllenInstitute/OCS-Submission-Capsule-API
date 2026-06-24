from __future__ import annotations

import pandas as pd
import pytest

from ocs_submission.ocs_command_builder import (
    COMMAND_RECORD_COLUMNS,
    build_alignment_job_command_record,
    build_ocs_command_args,
    build_ocs_job_submission_command,
    build_post_alignment_job_command_record,
    select_alignment_command_config,
)


def test_selects_first_matching_alignment_config(config):
    config["workflows"]["RTX"] = {
        "alignment_command_configs": [
            {
                "name": "first",
                "match": {"library_preps": ["10xV4"], "organisms": ["*"]},
                "command": ["ocs"],
                "arguments": [],
                "spacing": 1,
            },
            {
                "name": "second",
                "match": {"library_preps": ["10xV4"], "organisms": ["*"]},
                "command": ["ocs"],
                "arguments": [],
                "spacing": 1,
            },
        ]
    }

    selected = select_alignment_command_config(
        config=config,
        modality="RTX",
        library_prep_method_name="10xV4",
        organism_common_name="mouse",
    )

    assert selected["name"] == "first"


def test_select_alignment_config_rejects_unmatched_library_prep(config):
    with pytest.raises(ValueError, match="No MTX alignment command config found"):
        select_alignment_command_config(
            config=config,
            modality="MTX",
            library_prep_method_name="UNSUPPORTED_PREP",
            organism_common_name="mouse",
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
        email="BICore@alleninstitute.org",
        command_template=template,
    )

    assert command_args == [
        "ocs",
        "fastqs",
        "align",
        "tenx-arc",
        "--reference-names",
        "mouse_mtx_ref",
        "--load-names",
        "LOAD_1",
        "--notify",
        "BICore@alleninstitute.org",
        "--addopts",
        "--chemistry ARC-v1",
    ]
    assert spacing == 180


def test_build_ocs_command_args_uses_all_reference_fallback(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="human")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    command_args, _ = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email="BICore@alleninstitute.org",
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
            email="BICore@alleninstitute.org",
            command_template=template,
        )


@pytest.mark.parametrize(
    "ingest_status, align_status, force_submission, should_execute",
    [
        ("INGEST_COMPLETE", "NOT COMPLETED", None, True),
        ("NOT COMPLETED", "NOT COMPLETED", None, False),
        ("INGEST_COMPLETE", "COMPLETED", None, False),
        ("INGEST_COMPLETE", "IN_PROGRESS", None, False),
        ("INGEST_COMPLETE", "COMPLETED", "alignment", True),
        ("NOT COMPLETED", "NOT COMPLETED", "alignment", False),
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
        email="BICore@alleninstitute.org",
        force_submission=force_submission,
    )

    assert result["align_should_execute"] is should_execute
    assert (result["align_command_args"] is not None) is should_execute


@pytest.mark.parametrize(
    "align_status, postalign_status, alignment_should_execute, force_submission, should_execute",
    [
        ("COMPLETED", "NOT COMPLETED", False, None, True),
        ("NOT COMPLETED", "NOT COMPLETED", False, None, False),
        ("COMPLETED", "NOT COMPLETED", True, None, False),
        ("COMPLETED", "COMPLETED", False, None, False),
        ("COMPLETED", "IN_PROGRESS", False, None, False),
        ("COMPLETED", "COMPLETED", False, "post-alignment", True),
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
        email="BICore@alleninstitute.org",
        force_submission=force_submission,
        alignment_should_execute=alignment_should_execute,
    )

    assert result["postalign_should_execute"] is should_execute
    assert (result["postalign_command_args"] is not None) is should_execute


def test_post_alignment_requires_matching_library_prep(config, make_fastq_record):
    record = make_fastq_record(
        align_status="COMPLETED",
        postalign_status="NOT COMPLETED",
        library_prep_method_name="unsupported_prep",
    )

    result = build_post_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email="BICore@alleninstitute.org",
        force_submission=None,
        alignment_should_execute=False,
    )

    assert result["postalign_should_execute"] is False
    assert result["postalign_command_args"] is None


def test_build_ocs_job_submission_command_returns_manifest_schema(config, make_fastq_record):
    record = make_fastq_record(organism_common_name="mouse")

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record)]),
        modality="MTX",
        config=config,
        email="BICore@alleninstitute.org",
        force_submission=None,
        dry_run=True,
    )

    assert list(result.columns) == COMMAND_RECORD_COLUMNS
    assert result.loc[0, "fastq_name"] == "NY-MX22068-2"
    assert result.loc[0, "notify_email"] == "BICore@alleninstitute.org"
    assert result.loc[0, "align_should_execute"]
    assert not result.loc[0, "postalign_should_execute"]
