from __future__ import annotations

from ocs_submission.core.stages import Stage


def test_stage_vocabulary():
    """When listing pipeline stages, check the OCS result name and status column for each stage."""
    assert [
        (
            stage.ocs_stage_name,
            stage.ocs_list_results_arg,
            stage.fastq_status_column,
        )
        for stage in Stage
    ] == [
        ("ingest", "ingested-results", "ingest_status"),
        ("align", "aligned-results", "align_status"),
        ("postalign", "post-aligned-results", "postalign_status"),
    ]
