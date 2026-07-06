from __future__ import annotations

from pathlib import Path
from string import Formatter
from types import SimpleNamespace

import pytest

from ocs_submission.main import CONFIG_PATH, load_jsonc_config
from ocs_submission.ocs_command_builder import COMMAND_CONFIG_BY_STAGE, build_ocs_command_args, select_command_config
from ocs_submission.stages import Stage

EMAIL = "test@example.org"

# Loaded once for collection-time parametrization. Treated as read-only: tests that build
# commands load their own fresh copy so no test can mutate config state seen by another.
_DEFAULT_CONFIG = load_jsonc_config(CONFIG_PATH)


def _write(tmp_path: Path, text: str) -> str:
    config_path = tmp_path / "config.jsonc"
    config_path.write_text(text)
    return str(config_path)


def _library_preps(command_configs: list[dict]) -> set[str]:
    library_preps = set()
    for command_config in command_configs:
        library_preps.update(command_config["match"]["library_preps"])
    return library_preps


def _argument_placeholders(command_config: dict) -> set[str]:
    placeholders = set()
    for argument in command_config["arguments"]:
        value_template = argument.get("value", "")
        placeholders.update(field_name for _, field_name, _, _ in Formatter().parse(value_template) if field_name)
    return placeholders


def _fastq_record(library_prep_method_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        load_name="LOAD_1",
        library_prep_method_name=library_prep_method_name,
        organism_common_name="mouse",
    )


def test_load_jsonc_config_strips_comments(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            // a line comment
            "references": {
                "mouse": { "all": "mouse_ref" } /* a block comment */
            },
            "job_settings": { "limit": 5 }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert config["references"]["mouse"] == {"all": "mouse_ref"}
    assert config["job_settings"]["limit"] == 5


def test_load_jsonc_config_expands_pipe_delimited_reference_keys(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            "references": {
                "macaque | macaque_nemestrina | macaque_fascicularis": { "RTX": "macaque_ref" }
            }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert config["references"]["macaque"] == {"RTX": "macaque_ref"}
    assert config["references"]["macaque_nemestrina"] == {"RTX": "macaque_ref"}
    assert config["references"]["macaque_fascicularis"] == {"RTX": "macaque_ref"}


def test_single_organism_key_is_preserved(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            "references": {
                "human": { "all": "human_ref" }
            }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert set(config["references"]) == {"human"}


def _iter_default_command_configs():
    """Yield (modality, stage, command_config) for every command config in the production config.

    Enumerating at collection time turns each config entry into its own parametrized case, so a
    malformed entry names the exact modality/stage that broke. Indexing ``workflow`` by the
    expected field also means a missing field surfaces as a loud collection-time KeyError rather
    than a silently skipped iteration.
    """
    for modality, workflow in _DEFAULT_CONFIG["workflows"].items():
        for stage, (command_config_field, _) in COMMAND_CONFIG_BY_STAGE.items():
            for command_config in workflow[command_config_field]:
                yield modality, stage, command_config


def _command_config_params():
    params = [
        pytest.param(
            modality,
            stage,
            command_config,
            id=f"{modality}-{stage.name}-{command_config.get('name', 'unnamed')}",
        )
        for modality, stage, command_config in _iter_default_command_configs()
    ]
    assert params, "Default config produced no command configs to validate"
    return params


def _library_prep_params():
    params = [
        pytest.param(
            modality,
            stage,
            library_prep_method_name,
            id=f"{modality}-{stage.name}-{library_prep_method_name}",
        )
        for modality, stage, command_config in _iter_default_command_configs()
        for library_prep_method_name in command_config["match"]["library_preps"]
    ]
    assert params, "Default config produced no library preps to render"
    return params


def test_default_config_has_no_legacy_post_alignment_key():
    workflows = _DEFAULT_CONFIG["workflows"]
    assert workflows, "Default config declares no workflows"
    assert all("post_alignment" not in workflow for workflow in workflows.values())


@pytest.mark.parametrize("modality, stage, command_config", _command_config_params())
def test_default_command_config_match_is_well_formed(modality, stage, command_config):
    match = command_config["match"]
    assert match["library_preps"]
    assert "*" not in match["library_preps"]
    assert match.get("organisms") != ["*"]


@pytest.mark.parametrize(
    "modality, stage, library_prep_method_name, expected_command_prefix",
    [
        pytest.param("MTX", Stage.ALIGNMENT, "10xRSeq_Mult", ["ocs", "fastqs", "align", "tenx-arc"], id="mtx-align"),
        pytest.param(
            "MTX",
            Stage.POST_ALIGNMENT,
            "10xRSeq_Mult",
            ["ocs", "fastqs", "postalign", "tenx-arc"],
            id="mtx-postalign",
        ),
        pytest.param(
            "RTX",
            Stage.ALIGNMENT,
            "10xV4_FX4",
            ["ocs", "fastqs", "align", "tenx-rnaseq-multi"],
            id="rtx-align",
        ),
        pytest.param(
            "RTX",
            Stage.POST_ALIGNMENT,
            "10xV4_FX4",
            ["ocs", "fastqs", "postalign", "tenx-rnaseq"],
            id="rtx-postalign",
        ),
        pytest.param(
            "RFX",
            Stage.ALIGNMENT,
            "10xV4_FX16",
            ["ocs", "fastqs", "align", "tenx-rnaseq-multi"],
            id="rfx-align",
        ),
        pytest.param(
            "RFX",
            Stage.POST_ALIGNMENT,
            "10xV4_FX16",
            ["ocs", "fastqs", "postalign", "tenx-rnaseq"],
            id="rfx-postalign",
        ),
    ],
)
def test_default_config_builds_representative_commands(
    modality,
    stage,
    library_prep_method_name,
    expected_command_prefix,
):
    config = load_jsonc_config(CONFIG_PATH)
    command_config = select_command_config(
        config=config,
        modality=modality,
        stage=stage,
        library_prep_method_name=library_prep_method_name,
        organism_common_name="mouse",
    )

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=_fastq_record(library_prep_method_name),
        modality=modality,
        email=EMAIL,
        command_template=command_config,
    )

    assert command_args[: len(expected_command_prefix)] == expected_command_prefix
    assert "LOAD_1" in command_args
    assert EMAIL in command_args
    assert spacing > 0
    assert all("{" not in argument and "}" not in argument for argument in command_args)


@pytest.mark.parametrize("modality, stage, library_prep_method_name", _library_prep_params())
def test_default_config_renders_command_for_each_library_prep(modality, stage, library_prep_method_name):
    config = load_jsonc_config(CONFIG_PATH)
    selected_command_config = select_command_config(
        config=config,
        modality=modality,
        stage=stage,
        library_prep_method_name=library_prep_method_name,
        organism_common_name="mouse",
    )

    placeholders = _argument_placeholders(selected_command_config)
    if "execution_vcpus" in placeholders:
        assert selected_command_config.get("execution_vcpus")

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=_fastq_record(library_prep_method_name),
        modality=modality,
        email=EMAIL,
        command_template=selected_command_config,
    )

    assert spacing > 0
    assert all("{" not in argument and "}" not in argument for argument in command_args)


@pytest.mark.parametrize("modality", ["RTX", "RFX"])
def test_default_post_alignment_library_preps_match_alignment_library_preps(modality):
    workflow = load_jsonc_config(CONFIG_PATH)["workflows"][modality]
    alignment_preps = _library_preps(workflow["alignment_command_configs"])
    post_alignment_preps = _library_preps(workflow["post_alignment_command_configs"])

    assert post_alignment_preps == alignment_preps
