from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest


@pytest.fixture
def fastq_records() -> pd.DataFrame:
    # Saved manifest record plus the existing multiome example.
    return pd.read_csv(Path(__file__).parent / "fixtures" / "fastq_records.csv")


@pytest.fixture
def make_fastq_record(fastq_records) -> Callable[..., SimpleNamespace]:
    defaults = fastq_records.iloc[0].to_dict()

    def make_record(**overrides: object) -> SimpleNamespace:
        return SimpleNamespace(**(defaults | overrides))

    return make_record


@pytest.fixture
def config() -> dict[str, Any]:
    return {
        "references": {
            "mouse": {"MTX": "mouse_mtx_ref", "RTX": "mouse_rtx_ref"},
            "human": {"all": "human_all_ref"},
            "common-marmoset": {"MTX": "marmoset_mtx_ref"},
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
                "post_alignment_command_configs": [
                    {
                        "match": {"library_preps": ["10xMultX_GEX", "10xRSeq_Mult"]},
                        "command": ["ocs", "fastqs", "postalign", "tenx-arc"],
                        "arguments": [
                            {"flag": "--asset-name", "value": "10x_multiome_qc"},
                            {"flag": "--load-names", "value": "{load_name}"},
                        ],
                        "spacing": 60,
                    },
                ],
            },
        },
        "status_mappings": {
            "ingest_complete": ["INGEST_COMPLETE", "COMPLETED", "ARCHIVED"],
            "alignment_complete": ["COMPLETED", "ARCHIVED"],
            "post_alignment_complete": ["COMPLETED", "ARCHIVED"],
        },
    }
