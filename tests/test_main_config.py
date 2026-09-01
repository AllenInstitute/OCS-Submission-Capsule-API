from __future__ import annotations

from pathlib import Path
from string import Formatter
from types import SimpleNamespace

import pytest

from ocs_submission.commands.builder import COMMAND_CONFIG_BY_STAGE, build_ocs_command_args
from ocs_submission.main import CONFIG_PATH, load_jsonc_config

EMAIL = "test@example.org"
TEST_ORGANISM = "test-organism"

# Loaded once for collection-time parametrization. Treated as read-only: tests that build
# commands load their own fresh copy so no test can mutate config state seen by another.
_DEFAULT_CONFIG = load_jsonc_config(CONFIG_PATH)


def _write(tmp_path: Path, text: str) -> str:
    config_path = tmp_path / "config.jsonc"
    config_path.write_text(text)
    return str(config_path)


def _argument_placeholders(command_config: dict) -> set[str]:
    placeholders = set()
    for argument in command_config["arguments"]:
        value_template = argument.get("value", "")
        placeholders.update(field_name for _, field_name, _, _ in Formatter().parse(value_template) if field_name)
    return placeholders


def _fastq_record(library_prep_method_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        fastq_name="FASTQ_1",
        load_name="LOAD_1",
        library_prep_method_name=library_prep_method_name,
        organism_common_name=TEST_ORGANISM,
    )


def test_load_jsonc_config_strips_comments(tmp_path):
    """When loading a config file, check that line and block comments do not remove its JSON values."""
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
    """When a reference key has organism names separated by pipes, check that each organism gets an entry."""
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
    """When a reference key has one organism name, check that it stays as one entry."""
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


@pytest.mark.parametrize("_modality, _stage, command_config", _command_config_params())
def test_default_command_config_match_is_well_formed(_modality, _stage, command_config):
    """When reading command templates, check that each lists library preps without a wildcard."""
    match = command_config["match"]
    assert command_config["command"]
    assert match["library_preps"]
    assert "*" not in match["library_preps"]
    assert match.get("organisms") != ["*"]


@pytest.mark.parametrize("modality, _stage, command_config", _command_config_params())
def test_default_command_config_renders_without_unresolved_placeholders(modality, _stage, command_config):
    """When rendering a production command template, check that every placeholder is supported."""
    config = load_jsonc_config(CONFIG_PATH)
    library_prep_method_name = command_config["match"]["library_preps"][0]
    config["references"] = {TEST_ORGANISM: {"all": "test-reference"}}
    config["probe_sets_by_organism"] = {TEST_ORGANISM: "test-probe-set"}
    config["chemistry_by_library_prep"] = {library_prep_method_name: "test-chemistry"}

    placeholders = _argument_placeholders(command_config)
    if "execution_vcpus" in placeholders:
        assert command_config.get("execution_vcpus")

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=_fastq_record(library_prep_method_name),
        modality=modality,
        email=EMAIL,
        command_template=command_config,
    )

    assert spacing > 0
    assert all("{" not in argument and "}" not in argument for argument in command_args)
