from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

import pytest


@pytest.fixture
def make_fastq_record() -> Callable[..., SimpleNamespace]:
    def _make(**overrides: Any) -> SimpleNamespace:
        defaults: dict[str, Any] = {
            "fastq_name": "NY-MX22068-2",
            "study_set": "StudyA",
            "load_name": "LOAD_1",
            "library_prep_method_name": "10xRSeq_Mult",
            "organism_common_name": "mouse",
            "batch_name_from_vendor": "MTX-22068",
            "ingest_status": "INGEST_COMPLETE",
            "align_status": "NOT COMPLETED",
            "postalign_status": "NOT COMPLETED",
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    return _make


@pytest.fixture
def config() -> dict[str, Any]:
    return {
        "references": {
            "mouse": {"MTX": "mouse_mtx_ref", "RTX": "mouse_rtx_ref"},
            "human": {"all": "human_all_ref"},
        },
        "probe_sets_by_organism": {
            "mouse": {"10xV4_FX16": "mouse_probe_set"},
        },
        "chemistry_by_library_prep": {
            "10xRSeq_Mult": "ARC-v1",
            "10xV4": "SC3Pv4",
        },
        "workflows": {
            "MTX": {
                "alignment_command_configs": [
                    {
                        "name": "default",
                        "match": {
                            "library_preps": ["10xMultX_GEX", "10xRSeq_Mult"],
                            "organisms": ["*"],
                        },
                        "command": ["ocs", "fastqs", "align", "tenx-arc"],
                        "arguments": [
                            {"flag": "--reference-names", "value": "{reference_name}"},
                            {"flag": "--load-names", "value": "{load_name}"},
                            {"flag": "--notify", "value": "{email}"},
                        ],
                        "spacing": 180,
                    }
                ],
                "post_alignment": {
                    "match": {"library_preps": ["10xMultX_GEX", "10xRSeq_Mult"]},
                    "command": ["ocs", "fastqs", "postalign", "tenx-arc"],
                    "arguments": [
                        {"flag": "--asset-name", "value": "10x_multiome_qc"},
                        {"flag": "--load-names", "value": "{load_name}"},
                    ],
                    "spacing": 60,
                },
            },
        },
        "status_mappings": {
            "ingest_complete": ["INGEST_COMPLETE", "COMPLETED", "ARCHIVED"],
            "alignment_complete": ["COMPLETED", "ARCHIVED"],
            "post_alignment_complete": ["COMPLETED", "ARCHIVED"],
        },
    }
