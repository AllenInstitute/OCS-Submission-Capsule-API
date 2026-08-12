from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ocs_submission.audit import audit


@pytest.mark.parametrize(
    "batch_name, expected_sql_marker, expected_auditor, expected_modality",
    [
        pytest.param(
            "MTX-22068",
            "JOIN rna_amplification_inputs",
            audit.MTXAuditor,
            "MTX",
            id="mtx",
        ),
        pytest.param(
            "RTX-24047",
            "JOIN rna_amplification_inputs",
            audit.RTXAuditor,
            "RTX",
            id="rtx",
        ),
        pytest.param(
            "RFX-34056",
            "JOIN facs_wells_rseq_experiment_components",
            audit.RTXAuditor,
            "RFX",
            id="rfx",
        ),
    ],
)
def test__run_audit__uses_modality_query_and_rules(
    batch_name,
    expected_sql_marker,
    expected_auditor,
    expected_modality,
):
    """When auditing an MTX, RTX, or RFX batch, check that it uses the matching LIMS query and rules."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [(batch_name,)]
    cursor.description = [("batch_vendor_name",)]

    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    expected_report = pd.DataFrame({"result": ["Present"]})
    with (
        patch.object(audit.psycopg2, "connect", return_value=connection),
        patch.object(audit, "lims_database_username", return_value="user"),
        patch.object(audit, "lims_database_password", return_value="password"),
        patch.object(audit.MTXAuditor, "generate_report", return_value=expected_report) as mtx_report,
        patch.object(audit.RTXAuditor, "generate_report", return_value=expected_report) as rtx_report,
    ):
        lims_data, report, modality = audit.run_audit(batch_name)

    sql, query_params = cursor.execute.call_args.args
    assert expected_sql_marker in sql
    assert "rts.name_from_vendor = ANY(ARRAY[%(batch_names)s])" in sql
    assert query_params == {
        "batch_names": batch_name,
        "fastq_names": None,
        "load_names": None,
    }
    assert lims_data.to_dict(orient="records") == [{"batch_vendor_name": batch_name}]
    assert report is expected_report
    assert modality == expected_modality

    expected_report_mock = rtx_report if expected_auditor is audit.RTXAuditor else mtx_report
    other_report_mock = mtx_report if expected_auditor is audit.RTXAuditor else rtx_report
    expected_report_mock.assert_called_once()
    other_report_mock.assert_not_called()
    connection.close.assert_called_once_with()
