"""Read FASTQ metadata from LIMS2."""

from __future__ import annotations

import os
import urllib.parse
from pathlib import Path

import pandas as pd
import psycopg2

DB_CONNECTION_STRING_ENV_VAR = "DB_CONNECTION_STRING"
LIMS_SQL_PATH = Path(__file__).parent / "audit" / "rnaseq_and_multiome_lims_metadata_pull.sql"
LIMS_COLUMNS = {
    "exp_component_name": "fastq_name",
    "organism": "organism_common_name",
    "studies": "study_set",
    "load_name": "load_name",
    "lib_method": "library_prep_method_name",
    "batch_vendor_name": "batch_name_from_vendor",
}


def lims_connection_kwargs() -> dict[str, str | int]:
    connection_string = os.environ[DB_CONNECTION_STRING_ENV_VAR]
    parsed = urllib.parse.urlparse(connection_string.replace("postgresql+pg8000://", "postgresql://", 1))
    database = parsed.path.removeprefix("/")
    if parsed.scheme != "postgresql" or not parsed.hostname or not database:
        raise ValueError("DB_CONNECTION_STRING must be a PostgreSQL connection URI")

    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": database,
        "user": urllib.parse.unquote(parsed.username or ""),
        "password": urllib.parse.unquote(parsed.password or ""),
    }


def query_lims_metadata(
    *, fastq_names: list[str] | None = None, batch_name_from_vendor: str | None = None
) -> pd.DataFrame:
    """Return LIMS metadata for FASTQ names or one vendor batch."""
    if bool(fastq_names) == bool(batch_name_from_vendor):
        raise ValueError("Provide exactly one FASTQ-name list or vendor batch")

    if fastq_names and len(fastq_names) > 1:
        records = [query_lims_metadata(fastq_names=[fastq_name]) for fastq_name in fastq_names]
        return pd.concat(records)

    sql = LIMS_SQL_PATH.read_text()
    fastq_name = fastq_names[0] if fastq_names else None
    query_values = {
        "batch_names": batch_name_from_vendor,
        "fastq_names": fastq_name,
        "load_names": None,
    }
    with psycopg2.connect(**lims_connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, query_values)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]

    metadata = pd.DataFrame(rows, columns=columns)
    if metadata.empty:
        lookup = batch_name_from_vendor or ", ".join(fastq_names or [])
        raise ValueError(f"LIMS2 returned no metadata for {lookup!r}")

    metadata = metadata.rename(columns=LIMS_COLUMNS)
    metadata["study_set"] = metadata["study_set"].str.replace(", ", "+", regex=False)
    duplicate_names = metadata.loc[metadata.duplicated("fastq_name", keep=False), "fastq_name"].unique()
    if len(duplicate_names):
        raise ValueError(f"LIMS2 returned multiple metadata rows for FASTQ names: {sorted(duplicate_names)}")
    if fastq_names:
        requested = set(fastq_names)
        returned = set(metadata["fastq_name"])
        missing = requested - returned
        if missing:
            raise ValueError(f"LIMS2 returned no metadata for FASTQ names: {sorted(missing)}")
    return metadata.set_index("fastq_name", drop=False)
