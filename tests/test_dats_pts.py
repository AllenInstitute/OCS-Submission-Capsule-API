from types import SimpleNamespace

from ocs_submission.dats_pts import DatsPtsReader
from ocs_submission.stages import Stage


def test__DatsPtsReader__stage_results_follow_exact_fastq_asset_lineage():
    ingest_type = SimpleNamespace(id="ingest-type", name="prod-ocs-ingest")
    align_type = SimpleNamespace(id="align-type", name="prod-ocs-align")
    ingest_process = SimpleNamespace(id="ingest-process", type=ingest_type, state="SUCCESS", created_at=1)
    alignment_process = SimpleNamespace(id="alignment-process", type=align_type, state="SUCCESS", created_at=2)
    ingest_asset = SimpleNamespace(external_id="ingest-asset", type="DIGITAL_ASSET")
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
            return []

        def get_processes_by_assets(self, asset_ids, relationship, first):
            if asset_ids == ["other-asset"]:
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
    assert results[Stage.ALIGNMENT].status == "NOT COMPLETED"
    assert results[Stage.POST_ALIGNMENT].status == "NOT COMPLETED"
