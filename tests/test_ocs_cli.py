from subprocess import CompletedProcess

import pandas as pd

from ocs_submission import ocs_cli


def _submission_record(fastq_name: str) -> dict:
    return {
        "fastq_name": fastq_name,
        "align_should_execute": True,
        "postalign_should_execute": False,
        "dry_run": False,
        "align_command": "ocs fastqs align",
        "align_command_args": ["ocs", "fastqs", "align"],
        "align_spacing": None,
        "align_demand_id": None,
        "align_submission_success": None,
        "align_error_message": None,
        "align_executed_at": None,
    }


def test__execute_ocs_submission_commands__polls_pts_before_submitting_at_limit(monkeypatch):
    class FakeDatsPtsReader:
        def __init__(self):
            self.active_job_counts = iter([100, 99, 99, 98])

        def count_active_submission_jobs(self):
            return next(self.active_job_counts)

    sleep_calls = []
    monkeypatch.setattr(ocs_cli, "DatsPtsReader", FakeDatsPtsReader)
    monkeypatch.setattr(ocs_cli.time, "sleep", sleep_calls.append)
    monkeypatch.setattr(
        ocs_cli,
        "execute_ocs_cmd",
        lambda command_args: CompletedProcess(
            command_args, 0, '{"demand_status":"SUBMITTED","demand_execution":{"demand_id":"demand-1"}}', ""
        ),
    )

    records = pd.DataFrame([_submission_record("FASTQ_1"), _submission_record("FASTQ_2")])
    result = ocs_cli.execute_ocs_submission_commands(records, job_limit=100, poll_interval_hours=1)

    assert sleep_calls == [3600, 3600]
    assert result["align_demand_id"].tolist() == ["demand-1", "demand-1"]
    assert result["align_submission_success"].tolist() == [True, True]
