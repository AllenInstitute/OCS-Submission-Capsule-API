from __future__ import annotations

from ocs_submission.stages import Stage


def test_stage_vocabulary():
    """When listing pipeline stages, check the OCS name and status column."""
    assert [
        (
            stage.ocs_stage_name,
            stage.fastq_status_column,
        )
        for stage in Stage
    ] == [
        ("ingest", "ingest_status"),
        ("align", "align_status"),
        ("postalign", "postalign_status"),
    ]
