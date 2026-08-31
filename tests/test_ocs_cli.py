from __future__ import annotations

import json
import subprocess
from unittest.mock import patch

from ocs_submission.integrations.ocs_cli import query_metadata


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
