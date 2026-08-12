from __future__ import annotations

from ocs_submission.stages import Stage


def test_stage_vocabulary():
    """When listing pipeline stages, check the OCS result name, database name, and status column for each stage."""
    assert [
        (
            stage.ocs_stage_name,
            stage.ocs_list_results_arg,
            stage.running_db_stage_name,
            stage.fastq_status_column,
        )
        for stage in Stage
    ] == [
        ("ingest", "ingested-results", None, "ingest_status"),
        ("align", "aligned-results", "alignment", "align_status"),
        ("postalign", "post-aligned-results", "postqc", "postalign_status"),
    ]


def test_running_db_names_map_to_ocs_stage_names():
    """When reading a database job name, check that it maps to the correct OCS stage name."""
    assert {
        Stage.ALIGNMENT.running_db_stage_name: Stage.ALIGNMENT.ocs_stage_name,
        Stage.POST_ALIGNMENT.running_db_stage_name: Stage.POST_ALIGNMENT.ocs_stage_name,
    } == {
        "alignment": "align",
        "postqc": "postalign",
    }
