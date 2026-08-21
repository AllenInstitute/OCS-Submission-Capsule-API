from types import SimpleNamespace

from ocs_submission.dats_pts import DatsPtsReader
from ocs_submission.stages import Stage


def test__DatsPtsReader__stage_results_follow_exact_fastq_asset_lineage():
    ingest_type = SimpleNamespace(id="ingest-type", name="prod-ocs-ingest")
    align_type = SimpleNamespace(id="align-type", name="prod-ocs-align")
    ingest_process = SimpleNamespace(id="ingest-process", type=ingest_type, state="SUCCESS", created_at=1)
    alignment_process = SimpleNamespace(id="alignment-process", type=align_type, state="SUCCESS", created_at=2)
    ingest_asset = SimpleNamespace(external_id="ingest-asset", type="DIGITAL_ASSET")
    alignment_asset = SimpleNamespace(external_id="alignment-asset", type="DIGITAL_ASSET")
    other_asset = SimpleNamespace(external_id="other-asset", type="DIGITAL_ASSET")

    class FakePts:
        def get_process_types_by_name(self, name, version):
            process_type = ingest_type if name == "prod-ocs-ingest" else align_type
            return SimpleNamespace(nodes=[process_type])

        def get_processes_by_metadata(self, metadata_filter, first):
            return SimpleNamespace(nodes=[ingest_process])

        def get_process_outputs(self, process_id):
            if process_id == "ingest-process":
                return [ingest_asset, other_asset]
            if process_id == "alignment-process":
                return [alignment_asset]
            return []

        def get_processes_by_assets(self, asset_ids, relationship, first):
            if asset_ids == ["ingest-asset"]:
                return SimpleNamespace(nodes=[alignment_process])
            return SimpleNamespace(nodes=[])

    class FakeDats:
        def get_assets_by_ids(self, asset_ids, first):
            assets = {
                "ingest-asset": SimpleNamespace(
                    id="ingest-asset",
                    tags=["fastq_name: NW-FX38025-7"],
                    instances=[SimpleNamespace(download_url="s3://ingest")],
                ),
                "alignment-asset": SimpleNamespace(
                    id="alignment-asset",
                    tags=[],
                    instances=[SimpleNamespace(download_url="s3://alignment")],
                ),
                "other-asset": SimpleNamespace(
                    id="other-asset",
                    tags=["fastq_name: another-fastq"],
                    instances=[SimpleNamespace(download_url="s3://other")],
                ),
            }
            return SimpleNamespace(nodes=[assets[asset_id] for asset_id in asset_ids])

    reader = DatsPtsReader.__new__(DatsPtsReader)
    reader.pts = FakePts()
    reader.dats = FakeDats()

    results = reader.get_stage_results("NW-FX38025-7")

    assert results[Stage.INGEST].status == "COMPLETED"
    assert results[Stage.INGEST].location == "s3://ingest"
    assert results[Stage.ALIGNMENT].status == "COMPLETED"
    assert results[Stage.ALIGNMENT].location == "s3://alignment"
    assert results[Stage.POST_ALIGNMENT].status == "NOT COMPLETED"


def test__DatsPtsReader__counts_active_submission_processes_from_pts():
    class FakePts:
        def get_process_types_by_name(self, name, version):
            return SimpleNamespace(nodes=[SimpleNamespace(id=f"{name}-type")])

        def get_processes_by_type(self, process_type, state, first):
            return SimpleNamespace(total_count=1 if state in {"RUNNING", "IN_PROGRESS"} else 0)

    reader = DatsPtsReader.__new__(DatsPtsReader)
    reader.pts = FakePts()

    assert reader.count_active_submission_jobs() == 4
