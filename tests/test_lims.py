import pytest

from ocs_submission.lims import lims_connection_kwargs


def test__lims_connection_kwargs__accepts_pg8000_production_uri(monkeypatch):
    monkeypatch.setenv(
        "DB_CONNECTION_STRING",
        "postgresql+pg8000://reader@limsdb2.corp.alleninstitute.org:5432/lims2",
    )

    assert lims_connection_kwargs() == {
        "host": "limsdb2.corp.alleninstitute.org",
        "port": 5432,
        "database": "lims2",
        "user": "reader",
        "password": "",
    }


def test__lims_connection_kwargs__rejects_missing_database(monkeypatch):
    monkeypatch.setenv("DB_CONNECTION_STRING", "postgresql://user:password@localhost/")

    with pytest.raises(ValueError, match="PostgreSQL connection URI"):
        lims_connection_kwargs()
