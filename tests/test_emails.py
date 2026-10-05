from unittest.mock import patch

import pandas as pd
import pytest

from ocs_submission.commands.builder import COMMAND_RECORD_COLUMNS
from ocs_submission.integrations import email as emails

EMAIL = "BICore@alleninstitute.org"


def _manifest_row(record, **overrides: object) -> dict:
    return {
        **dict.fromkeys(COMMAND_RECORD_COLUMNS),
        **vars(record),
        "dry_run": False,
        "align_should_execute": False,
        "align_library_prep_unconfigured": False,
        "postalign_should_execute": False,
        "postalign_library_prep_unconfigured": False,
        **overrides,
    }


@pytest.fixture
def send_email():
    with patch.object(emails, "send_email", autospec=True, return_value="message-id") as mock:
        yield mock


def test_summary_email_reports_unconfigured_library_preps(send_email, make_fastq_record, fastq_records):
    manifest = pd.DataFrame(
        [
            _manifest_row(make_fastq_record(), align_library_prep_unconfigured=True),
            _manifest_row(
                make_fastq_record(**fastq_records.iloc[2].to_dict()), postalign_library_prep_unconfigured=True
            ),
        ],
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    send_email.assert_called_once()
    assert send_email.call_args.kwargs["email"] == EMAIL
    body = send_email.call_args.kwargs["body"]
    assert "Failed Submissions: 0" in body
    assert "library prep names not matching the configuration file: NW-MX32013-2, NW-MX32021-10" in body
    assert send_email.call_args.kwargs["subject"] == "OCS Job Submission Summary"


def test_summary_email_omits_report_line_when_all_configured(send_email, make_fastq_record):
    manifest = pd.DataFrame(
        [
            _manifest_row(
                make_fastq_record(),
                align_should_execute=True,
                align_submission_success=True,
                align_command="ocs fastqs align",
                align_demand_id="demand-1",
                align_executed_at="2026-07-15 07:00:37",
            )
        ],
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    send_email.assert_called_once()
    assert "not matching the configuration file" not in send_email.call_args.kwargs["body"]


def test_summary_email_sends_when_only_unconfigured_preps(send_email, make_fastq_record):
    manifest = pd.DataFrame(
        [_manifest_row(make_fastq_record(), align_library_prep_unconfigured=True)],
    )

    emails.send_command_summary_email(ocs_job_commands_df=manifest, notify_email=EMAIL)

    send_email.assert_called_once()
    body = send_email.call_args.kwargs["body"]
    assert "Total Jobs Processed: 0" in body
    assert "Batch Name: MTX-32013" in body
    assert "library prep names not matching the configuration file: NW-MX32013-2" in body
