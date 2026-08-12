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
    unconfigured_library_prep_fastq_names,
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
    align_library_prep_unconfigured: bool = False,
    align_command_args: list[str] | None = None,
    align_spacing: int | None = None,
    postalign_should_execute: bool = False,
    postalign_library_prep_unconfigured: bool = False,
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
        "align_library_prep_unconfigured": align_library_prep_unconfigured,
        "align_command_args": align_command_args,
        "align_command": " ".join(align_command_args) if align_command_args else None,
        "align_spacing": align_spacing,
        "align_demand_id": None,
        "align_submission_success": None,
        "align_error_message": None,
        "align_executed_at": None,
        "postalign_should_execute": postalign_should_execute,
        "postalign_library_prep_unconfigured": postalign_library_prep_unconfigured,
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
    """When two command templates match a fastq sample, check that it uses the first template."""
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
    """When a command template has no organism list, check that it works for any fastq sample organism."""
    selected = select_command_config(
        config=config,
        modality="MTX",
        stage=Stage.ALIGNMENT,
        library_prep_method_name="10xRSeq_Mult",
        organism_common_name="human",
    )

    assert selected["name"] == "default"


def test_select_command_config_skips_configs_restricted_to_other_organisms(config):
    """When choosing a command template, check that a fastq sample skips templates for other organisms."""
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
    "stage",
    [
        pytest.param(Stage.ALIGNMENT, id="alignment"),
        pytest.param(Stage.POST_ALIGNMENT, id="post_alignment"),
    ],
)
def test_select_command_config_returns_none_for_unlisted_library_prep(config, stage):
    """When a fastq sample library prep is not configured, check that it has no command template."""
    selected = select_command_config(
        config=config,
        modality="MTX",
        stage=stage,
        library_prep_method_name="UNSUPPORTED_PREP",
        organism_common_name="mouse",
    )

    assert selected is None


@pytest.mark.parametrize("missing_field", ["match", "library_preps"])
def test_select_command_config_reports_missing_library_preps_config(config, missing_field):
    """When a command template has no library preps, check that it raises a configuration error."""
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
    """When a fastq sample library prep has no template for its organism, check that it raises an error."""
    config["workflows"]["MTX"]["post_alignment_command_configs"] = [
        _command_config("mouse", ["10xRSeq_Mult"], organisms=["mouse"])
    ]

    with pytest.raises(ValueError, match="No MTX post-alignment command config found .* organism rat"):
        select_command_config(
            config=config,
            modality="MTX",
            stage=Stage.POST_ALIGNMENT,
            library_prep_method_name="10xRSeq_Mult",
            organism_common_name="rat",
        )


def test_build_ocs_command_args_renders_template_values(config, make_fastq_record):
    """When building a command, check that a fastq sample fills its reference, load name, email, and chemistry values."""
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
    """When building a command, check that it includes the probe set, CPU count, and a flag with no value."""
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


def test_build_ocs_command_args_uses_shared_organism_probe_set(config, make_fastq_record):
    """When an organism has one shared probe set, check that its fastq sample uses that probe set."""
    config["probe_sets_by_organism"]["human"] = "human_probe_set"
    template = _command_config(
        name="cellflex",
        library_preps=["10xV4_FX16"],
        command=["ocs"],
        arguments=[{"flag": "--probe-set", "value": "{probe_set}"}],
    )
    record = make_fastq_record(
        organism_common_name="human",
        library_prep_method_name="10xV4_FX16",
    )

    command_args, _ = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert command_args == ["ocs", "--probe-set", "human_probe_set"]


def test_build_ocs_command_args_uses_empty_values_for_unknown_chemistry_and_probe_set(config, make_fastq_record):
    """When chemistry and a probe set are not configured, check that the command uses empty values."""
    template = {
        **config["workflows"]["MTX"]["alignment_command_configs"][0],
        "arguments": [
            {"flag": "--chemistry", "value": "{chemistry}"},
            {"flag": "--probe-set", "value": "{probe_set}"},
        ],
    }
    record = make_fastq_record(library_prep_method_name="10xMultX_GEX")

    command_args, _ = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert command_args == ["ocs", "fastqs", "align", "tenx-arc", "--chemistry", "", "--probe-set", ""]


def test_build_ocs_command_args_uses_all_reference_fallback(config, make_fastq_record):
    """When a fastq sample modality has no reference, check that it uses the `all` reference."""
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


@pytest.mark.parametrize(
    "library_prep_method_name, expected_reference_name",
    [
        pytest.param("10xRSeq_Mult", "mouse_mtx_ref", id="existing_prep"),
        pytest.param("10xV4", "mouse_mtx_v4_ref", id="new_prep"),
    ],
)
def test_build_ocs_command_args_uses_library_prep_specific_reference(
    config,
    make_fastq_record,
    library_prep_method_name,
    expected_reference_name,
):
    """When references are mapped by library prep, check that a fastq sample uses its library prep reference."""
    config["references"]["mouse"]["MTX"] = {
        "library_preps": {
            "10xRSeq_Mult": "mouse_mtx_ref",
            "10xV4": "mouse_mtx_v4_ref",
        }
    }
    record = make_fastq_record(library_prep_method_name=library_prep_method_name)
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    command_args, _ = build_ocs_command_args(
        config=config,
        fastq_record=record,
        modality="MTX",
        email=EMAIL,
        command_template=template,
    )

    assert expected_reference_name in command_args


def test_build_ocs_command_args_requires_library_prep_specific_reference(config, make_fastq_record):
    """When a fastq sample library prep has no reference, check that it raises an error."""
    config["references"]["mouse"]["MTX"] = {
        "library_preps": {
            "another_prep": "mouse_other_ref",
        }
    }
    record = make_fastq_record(library_prep_method_name="10xRSeq_Mult")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    with pytest.raises(
        KeyError,
        match="No reference for organism 'mouse', modality 'MTX', and library prep '10xRSeq_Mult'",
    ):
        build_ocs_command_args(
            config=config,
            fastq_record=record,
            modality="MTX",
            email=EMAIL,
            command_template=template,
        )


def test_build_ocs_command_args_requires_valid_library_prep_reference_mapping(config, make_fastq_record):
    """When a reference configuration is invalid, check that building the command raises an error."""
    config["references"]["mouse"]["MTX"] = {}
    record = make_fastq_record(library_prep_method_name="10xRSeq_Mult")
    template = config["workflows"]["MTX"]["alignment_command_configs"][0]

    with pytest.raises(KeyError, match="must be a reference name or contain a 'library_preps' mapping"):
        build_ocs_command_args(
            config=config,
            fastq_record=record,
            modality="MTX",
            email=EMAIL,
            command_template=template,
        )


def test_build_ocs_command_args_requires_matching_reference(config, make_fastq_record):
    """When a fastq sample modality has no reference, check that building the command raises an error."""
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
    """When a fastq sample organism has no reference configuration, check that building the command raises an error."""
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
    """When building an alignment submission command, check that the fastq sample ingest is complete and alignment is not complete or running."""
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


def test_alignment_skips_unconfigured_library_prep(config, make_fastq_record):
    """When alignment is needed but a fastq sample library prep has no command, check that it is not submitted."""
    record = make_fastq_record(library_prep_method_name="unsupported_prep")

    result = build_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
    )

    assert result["align_should_execute"] is False
    assert result["align_library_prep_unconfigured"] is True
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
    """When building a post-alignment submission command, check that the fastq sample alignment is complete and post-alignment is not complete or running."""
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


def test_post_alignment_skips_unconfigured_library_prep(config, make_fastq_record):
    """When post-alignment is needed but a fastq sample library prep has no command, check that it is not submitted."""
    record = make_fastq_record(
        align_status="COMPLETED",
        postalign_status="NOT COMPLETED",
        library_prep_method_name="unsupported_prep",
    )

    result = build_post_alignment_job_command_record(
        fastq_record=record,
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        alignment_should_execute=False,
    )

    assert result["postalign_should_execute"] is False
    assert result["postalign_library_prep_unconfigured"] is True
    _assert_job_not_scheduled(result, "postalign")


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
    """When post-alignment is not needed, check that a fastq sample with an unsupported library prep does not fail."""
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
    """When a fastq sample library prep has only an alignment command, check that it gets no post-alignment command."""
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


def test_build_ocs_job_submission_command_allows_forced_alignment_without_post_alignment_config(
    config,
    make_fastq_record,
):
    """When alignment is forced for a fastq sample with no post-alignment command, check that only alignment is built."""
    config["workflows"]["MTX"]["alignment_command_configs"][0]["match"]["library_preps"].append("align_only_prep")
    record = make_fastq_record(
        align_status="COMPLETED",
        postalign_status="NOT COMPLETED",
        library_prep_method_name="align_only_prep",
    )

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record)]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission="alignment",
        dry_run=True,
    )

    assert bool(result.at[0, "align_should_execute"]) is True
    assert bool(result.at[0, "postalign_should_execute"]) is False
    assert result.at[0, "align_command_args"] == EXPECTED_ALIGNMENT_COMMAND_ARGS
    assert result.at[0, "postalign_command_args"] is None


def test_build_ocs_job_submission_command_returns_expected_manifest_row(config, make_fastq_record):
    """When building a submission manifest for one fastq sample, check that the row has its alignment command and metadata."""
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
    """When a fastq sample alignment is complete, check that its manifest row has a post-alignment command."""
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
    """When building one manifest for two fastq samples, check that it can contain alignment and post-alignment commands."""
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


def test_build_ocs_job_submission_command_flags_unconfigured_library_prep(config, make_fastq_record):
    """When a fastq sample library prep is not configured, check that it is skipped and returned in the skipped list."""
    records = [
        make_fastq_record(fastq_name="configured"),
        make_fastq_record(fastq_name="unconfigured", library_prep_method_name="unsupported_prep"),
    ]

    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(record) for record in records]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    assert bool(result.at[0, "align_should_execute"]) is True
    assert bool(result.at[0, "align_library_prep_unconfigured"]) is False
    assert bool(result.at[1, "align_should_execute"]) is False
    assert bool(result.at[1, "align_library_prep_unconfigured"]) is True
    assert unconfigured_library_prep_fastq_names(result) == ["unconfigured"]


def test_unconfigured_library_prep_fastq_names_empty_when_all_configured(config, make_fastq_record):
    """When every fastq sample library prep has a command, check that no samples are returned as skipped."""
    result = build_ocs_job_submission_command(
        fastq_records_df=pd.DataFrame([vars(make_fastq_record())]),
        modality="MTX",
        config=config,
        email=EMAIL,
        force_submission=None,
        dry_run=True,
    )

    assert unconfigured_library_prep_fastq_names(result) == []


def test_build_ocs_job_submission_command_returns_empty_manifest_with_schema(config):
    """When there are no fastq samples, check that the manifest is empty and has the normal output columns."""
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
