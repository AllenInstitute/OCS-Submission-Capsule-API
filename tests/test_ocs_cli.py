from __future__ import annotations

import json
import logging
import subprocess
from unittest.mock import patch

import pandas as pd
import pytest

from ocs_submission.integrations.ocs_cli import execute_ocs_submission_commands, query_metadata


@pytest.mark.parametrize(
    "input_flag, input_name",
    [
        pytest.param("--load-names", "3592-10_A01", id="load_name"),
        pytest.param("--fastq-names", "NW-FX8012-1", id="fastq_name"),
    ],
)
def test__execute_ocs_submission_commands__logs_command_input_name(
    caplog,
    input_flag,
    input_name,
):
    """When dry-running a command, check that its log identifies the submitted input."""
    command_args = ["ocs", "fastqs", "postalign", "tenx-rnaseq", input_flag, input_name]
    manifest = pd.DataFrame(
        [
            {
                "align_should_execute": False,
                "postalign_should_execute": True,
                "dry_run": True,
                "fastq_name": "NW-FX8012-1",
                "load_name": "3592-10_A01",
                "postalign_command": " ".join(command_args),
                "postalign_command_args": command_args,
            }
        ]
    )

    with caplog.at_level(logging.INFO, logger="ocs_submission.integrations.ocs_cli"):
        execute_ocs_submission_commands(manifest, job_limit=10)

    assert f"Dry run postalign for {input_name}:" in caplog.text


def test__query_metadata__looks_up_each_load_name():
    metadata_rows = [
        {
            "fastq_name": "NW-MX32013-2",
            "studies": ["MG_MethDev", "MultiomeGEMX_pilot"],
            "load_name": "3492_A01",
            "organism_common_name": "mouse",
            "library_prep_method_name": "10xMultX_GEX",
            "batch_name_from_vendor": "MTX-32013",
        }
    ]
    completed_process = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(metadata_rows),
        stderr="",
    )

    with patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", return_value=completed_process) as execute:
        result = query_metadata(load_name_list=["3492_A01"])

    command = execute.call_args.kwargs["cmd_list"]
    assert command[command.index("--load-name") + 1] == "3492_A01"
    assert result.loc["NW-MX32013-2", "load_name"] == "3492_A01"


def test__query_metadata__looks_up_each_fastq_name_with_singular_flag():
    metadata_rows = [
        {
            "fastq_name": "NW-MX32013-2",
            "studies": ["MG_MethDev"],
            "load_name": "3492_A01",
            "organism_common_name": "mouse",
            "library_prep_method_name": "10xMultX_GEX",
            "batch_name_from_vendor": "MTX-32013",
        }
    ]
    completed_process = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(metadata_rows),
        stderr="",
    )

    with patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", return_value=completed_process) as execute:
        result = query_metadata(fastq_name_list=["NW-MX32013-2"])

    command = execute.call_args.kwargs["cmd_list"]
    assert command[command.index("--fastq-name") + 1] == "NW-MX32013-2"
    assert "--fastq-names" not in command
    assert result.loc["NW-MX32013-2", "load_name"] == "3492_A01"
