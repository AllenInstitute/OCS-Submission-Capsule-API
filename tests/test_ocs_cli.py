import json
import logging
import subprocess
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ocs_submission.commands.builder import build_ocs_job_submission_command
from ocs_submission.integrations.ocs_cli import execute_ocs_submission_commands, query_metadata


@pytest.mark.parametrize(
    "input_flag, input_name",
    [
        pytest.param("--load-names", "3492_A01", id="load_name"),
        pytest.param("--fastq-names", "NW-MX32013-2", id="fastq_name"),
    ],
)
def test__execute_ocs_submission_commands__logs_command_input_name(caplog, make_fastq_record, input_flag, input_name):
    command_args = ["ocs", "fastqs", "postalign", "tenx-arc", input_flag, input_name]
    manifest = pd.DataFrame(
        [
            {
                **vars(make_fastq_record()),
                "align_should_execute": False,
                "postalign_should_execute": True,
                "dry_run": True,
                "postalign_command": " ".join(command_args),
                "postalign_command_args": command_args,
            }
        ]
    )

    with (
        caplog.at_level(logging.INFO, logger="ocs_submission.integrations.ocs_cli"),
        patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", autospec=True) as execute,
    ):
        execute_ocs_submission_commands(manifest, job_limit=10)

    assert f"Dry run postalign for {input_name}:" in caplog.text
    execute.assert_not_called()


@pytest.mark.parametrize(
    "lookup_argument, lookup_column, lookup_flag",
    [
        pytest.param("load_name_list", "load_name", "--load-name", id="load_names"),
        pytest.param("fastq_name_list", "fastq_name", "--fastq-name", id="fastq_names"),
    ],
)
def test__query_metadata__looks_up_each_name_with_singular_flag(
    fastq_records, lookup_argument, lookup_column, lookup_flag
):
    metadata = fastq_records.drop(columns=["ingest_status", "align_status", "postalign_status"]).rename(
        columns={"study_set": "studies"}
    )
    metadata["studies"] = metadata["studies"].str.split("+")
    names = metadata[lookup_column].unique().tolist()
    responses = [
        subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(metadata[metadata[lookup_column] == name].to_dict(orient="records")),
        )
        for name in names
    ]

    with patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", autospec=True, side_effect=responses) as execute:
        result = query_metadata(**{lookup_argument: names})

    assert execute.call_count == len(names)
    for call, name in zip(execute.call_args_list, names, strict=True):
        command = call.kwargs["cmd_list"]
        assert command[-2:] == [lookup_flag, name]
        assert f"{lookup_flag}s" not in command
    expected = metadata.assign(study_set=fastq_records["study_set"]).set_index("fastq_name", drop=False)
    assert_frame_equal(result, expected)


def test__query_metadata__missing_load_metadata_asks_to_check_load_names():
    response = subprocess.CompletedProcess(args=[], returncode=0, stdout="[]")

    with patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", autospec=True, return_value=response):
        with pytest.raises(ValueError) as error:
            query_metadata(load_name_list=["NW-FX38024-3"])

    assert str(error.value) == (
        "OCS returned no metadata for load 'NW-FX38024-3'. "
        "There may be an issue with this load name on OCS — verify metadata with "
        "`ocs fastqs list metadata` and perform a manual check. "
        "Check that these are load names, not fastq sample names."
    )


@pytest.mark.parametrize(
    "align_status, stage",
    [
        pytest.param("NOT COMPLETED", "align", id="alignment"),
        pytest.param("COMPLETED", "postalign", id="post_alignment"),
    ],
)
@pytest.mark.parametrize(
    "demand_status, success",
    [
        pytest.param("SUBMITTED", True, id="submitted"),
        pytest.param("FAILED", False, id="failed"),
    ],
)
def test__execute_ocs_submission_commands__records_submission_result(
    config, make_fastq_record, align_status, stage, demand_status, success
):
    manifest = build_ocs_job_submission_command(
        pd.DataFrame([vars(make_fastq_record(align_status=align_status))]),
        modality="MTX",
        config=config,
        email="test@example.org",
        force_submission=None,
        dry_run=False,
    )
    response = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps({"demand_status": demand_status, "demand_execution": {"demand_id": "demand-1"}}),
    )

    with (
        patch("ocs_submission.integrations.ocs_cli.execute_ocs_cmd", autospec=True, return_value=response) as execute,
        patch("ocs_submission.integrations.ocs_cli.can_submit_job", autospec=True, return_value=True),
    ):
        result = execute_ocs_submission_commands(manifest, job_limit=10)

    execute.assert_called_once_with(manifest.at[0, f"{stage}_command_args"])
    assert result is manifest
    assert result.at[0, f"{stage}_submission_success"] == success
    assert result.at[0, f"{stage}_demand_id"] == ("demand-1" if success else None)
    assert result.at[0, f"{stage}_error_message"] == (None if success else "Job submission failed")
    assert result.at[0, f"{stage}_executed_at"]


def test__execute_ocs_submission_commands__records_command_failure(config, make_fastq_record):
    manifest = build_ocs_job_submission_command(
        pd.DataFrame([vars(make_fastq_record())]),
        modality="MTX",
        config=config,
        email="test@example.org",
        force_submission=None,
        dry_run=False,
    )

    with (
        patch(
            "ocs_submission.integrations.ocs_cli.execute_ocs_cmd",
            autospec=True,
            side_effect=subprocess.CalledProcessError(1, manifest.at[0, "align_command_args"]),
        ),
        patch("ocs_submission.integrations.ocs_cli.can_submit_job", autospec=True, return_value=True),
    ):
        result = execute_ocs_submission_commands(manifest, job_limit=10)

    assert not result.at[0, "align_submission_success"]
    assert result.at[0, "align_demand_id"] is None
    assert result.at[0, "align_error_message"].startswith("Command execution failed:")
    assert result.at[0, "align_executed_at"]
