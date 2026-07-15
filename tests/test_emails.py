from __future__ import annotations

import pandas as pd

from ocs_submission import emails
from ocs_submission.ocs_command_builder import COMMAND_RECORD_COLUMNS

EMAIL = "BICore@alleninstitute.org"


def _manifest_row(**overrides: object) -> dict:
    row = dict.fromkeys(COMMAND_RECORD_COLUMNS)
    row.update(
        {
            "fastq_name": "NY-MX22068-2",
            "load_name": "LOAD_1",
            "batch_name_from_vendor": "RTX-24047",
            "dry_run": False,
            "align_should_execute": False,
            "align_library_prep_unconfigured": False,
            "postalign_should_execute": False,
            "postalign_library_prep_unconfigured": False,
            "align_submission_success": None,
            "postalign_submission_success": None,
        }
    )
    row.update(overrides)
    return row


def _capture_body(monkeypatch) -> list[str]:
    sent_bodies: list[str] = []

    def fake_send_email(email: str, subject: str, body: str, attachment_paths=None) -> str:
        sent_bodies.append(body)
        return "message-id"

    monkeypatch.setattr(emails, "send_email", fake_send_email)
    return sent_bodies


def test_summary_email_reports_unconfigured_library_preps(monkeypatch):
    sent_bodies = _capture_body(monkeypatch)
    manifest = pd.DataFrame(
        [
            _manifest_row(fastq_name="x", align_library_prep_unconfigured=True),
            _manifest_row(fastq_name="y", postalign_library_prep_unconfigured=True),
        ],
        columns=COMMAND_RECORD_COLUMNS,
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    assert len(sent_bodies) == 1
    assert (
        "\n".join(
            [
                "Failed Submissions: 0",
                "Batch Name: RTX-24047",
                "The following Fastq Name have library prep names not matching the configuration file: x, y",
            ]
        )
        in sent_bodies[0]
    )


def test_summary_email_omits_report_line_when_all_configured(monkeypatch):
    sent_bodies = _capture_body(monkeypatch)
    manifest = pd.DataFrame(
        [
            _manifest_row(
                align_should_execute=True,
                align_submission_success=True,
                align_command="ocs fastqs align",
                align_demand_id="demand-1",
                align_executed_at="2026-07-15 07:00:37",
            )
        ],
        columns=COMMAND_RECORD_COLUMNS,
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    assert len(sent_bodies) == 1
    assert "not matching the configuration file" not in sent_bodies[0]


def test_summary_email_sends_when_only_unconfigured_preps(monkeypatch):
    sent_bodies = _capture_body(monkeypatch)
    manifest = pd.DataFrame(
        [_manifest_row(fastq_name="x", align_library_prep_unconfigured=True)],
        columns=COMMAND_RECORD_COLUMNS,
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    assert len(sent_bodies) == 1
    assert "Total Jobs Processed: 0" in sent_bodies[0]
    assert "library prep names not matching the configuration file: x" in sent_bodies[0]
