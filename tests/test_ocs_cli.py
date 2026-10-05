import json
import logging
import subprocess
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

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
