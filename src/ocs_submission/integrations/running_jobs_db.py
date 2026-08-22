"""PostgreSQL helpers for the running-jobs table.

Track jobs submitted by this capsule so later runs can recheck their status, including before
OCS has produced a result for a pipeline stage.
"""

from __future__ import annotations

import json
import subprocess

from psycopg2 import OperationalError, pool
from psycopg2.extras import RealDictCursor

from ..core.stages import Stage
from .environment import running_jobs_db_url

_connection_pool: pool.ThreadedConnectionPool | None = None


def init_connection_pool(min_conn=1, max_conn=5):
    """
    Create the shared tracker-DB connection pool on first call and reuse it thereafter.

    Parameters:
    min_conn: The minimum number of connections to keep open against ``RUNNING_JOBS_DB_URL``.
    max_conn: The maximum number of connections to keep open against ``RUNNING_JOBS_DB_URL``.

    Returns:
    Return the shared ``pool.ThreadedConnectionPool``.
    """
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            running_jobs_db_url(),
        )
    return _connection_pool


def get_connection():
    """
    Replace an idle connection if the server closed it during a long job wait.

    Returns:
    A connection borrowed from the shared connection pool.
    """
    connection_pool = init_connection_pool()
    conn = connection_pool.getconn()
    try:
        conn.rollback()
    except OperationalError:
        connection_pool.putconn(conn, close=True)
        conn = connection_pool.getconn()
    return conn


def return_connection(conn):
    """
    Return a connection to the pool.

    Parameters:
    conn: A connection previously obtained from ``get_connection``.
    """
    # get_connection() initializes the pool before returning a connection.
    assert _connection_pool is not None
    _connection_pool.putconn(conn)


def add_job(
    fastq_name: str,
    running_db_stage_name: str,
    command: str,
    demand_id: str,
    status: str = "SUBMITTED",
    batch_name_from_vendor: str | None = None,
):
    """
    Insert or update the ``running_jobs`` row for a FASTQ sample and stage.

    Parameters:
    fastq_name: The FASTQ name identifying the job.
    running_db_stage_name: The tracker-DB stage name, either ``alignment`` or ``postqc``.
    command: The submitted command to record.
    demand_id: The OCS demand id to record.
    status: The job status to record, defaulting to ``SUBMITTED``.
    batch_name_from_vendor: The optional vendor batch name to record.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM running_jobs WHERE fastq_name = %s AND job_type = %s",
        (fastq_name, running_db_stage_name),
    )
    existing = cursor.fetchone()

    if existing:
        cursor.execute(
            "UPDATE running_jobs SET command = %s, demand_id = %s, status = %s, "
            "batch_name_from_vendor = %s, updated_at = NOW() "
            "WHERE fastq_name = %s AND job_type = %s",
            (
                command,
                demand_id,
                status,
                batch_name_from_vendor,
                fastq_name,
                running_db_stage_name,
            ),
        )
    else:
        cursor.execute(
            "INSERT INTO running_jobs "
            "(fastq_name, job_type, command, demand_id, status, batch_name_from_vendor) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            (
                fastq_name,
                running_db_stage_name,
                command,
                demand_id,
                status,
                batch_name_from_vendor,
            ),
        )

    conn.commit()
    cursor.close()
    return_connection(conn)


def get_job(fastq_name: str, running_db_stage_name: str) -> dict | None:
    """
    Return one ``running_jobs`` row for a FASTQ sample and stage.

    Parameters:
    fastq_name: The FASTQ name identifying the job.
    running_db_stage_name: The tracker-DB stage name, either ``alignment`` or ``postqc``.

    Returns:
    The matching row as a dict, or ``None`` if no such row exists.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute(
        "SELECT id, fastq_name, job_type, command, demand_id, status, "
        "batch_name_from_vendor, created_at, updated_at "
        "FROM running_jobs WHERE fastq_name = %s AND job_type = %s",
        (fastq_name, running_db_stage_name),
    )
    result = cursor.fetchone()
    cursor.close()
    return_connection(conn)

    return result


def check_job_status(fastq_name: str, stage: Stage) -> str | None:
    """
    Check a tracked job's OCS status and save the new status.

    Parameters:
    fastq_name: The FASTQ name identifying the job.
    stage: The pipeline stage to check.

    Returns:
    The latest status, or ``None`` if the job is not tracked or OCS reports no status.
    """
    running_db_stage_name = stage.running_db_stage_name
    if running_db_stage_name is None:
        raise ValueError(f"Stage {stage.name} is not tracked in the running-jobs database")

    job = get_job(fastq_name, running_db_stage_name)

    if not job:
        return None

    demand_id = job["demand_id"]
    cmd = [
        "ocs",
        "fastqs",
        stage.ocs_stage_name,
        "get-status",
        "--demand-id",
        demand_id,
        "--format",
        "json",
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)

    if not result.stdout.strip():
        return None

    status_data = json.loads(result.stdout)
    if not status_data:
        return None

    status = status_data[0].get("status")
    if status:
        update_job_status(fastq_name, running_db_stage_name, status)
    return status


def update_job_status(fastq_name: str, running_db_stage_name: str, status: str):
    """
    Save a new status and update ``updated_at`` for a tracked job.

    Parameters:
    fastq_name: The FASTQ name identifying the job.
    running_db_stage_name: The tracker-DB stage name, either ``alignment`` or ``postqc``.
    status: The new status to write.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE running_jobs SET status = %s, updated_at = NOW() WHERE fastq_name = %s AND job_type = %s",
        (status, fastq_name, running_db_stage_name),
    )
    conn.commit()
    cursor.close()
    return_connection(conn)
