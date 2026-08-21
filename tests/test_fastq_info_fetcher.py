import pandas as pd

from ocs_submission.fastq_info_fetcher import log_fastq_status_details


def test__log_fastq_status_details__logs_each_stage_for_each_fastq(caplog):
    records = pd.DataFrame(
        [
            {
                "fastq_name": "NY-TX24048-1",
                "ingest_status": "COMPLETED",
                "align_status": "IN_PROGRESS",
                "postalign_status": "NOT COMPLETED",
            }
        ]
    )

    with caplog.at_level("INFO"):
        log_fastq_status_details(records)

    assert caplog.messages == [
        "Checking Status for NY-TX24048-1",
        "  - Ingest Status: COMPLETED",
        "  - Align Status: IN_PROGRESS",
        "  - Postalign Status: NOT COMPLETED",
    ]
