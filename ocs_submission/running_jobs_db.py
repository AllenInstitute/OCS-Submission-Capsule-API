import os
import json
import subprocess

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


_connection_pool = None


def init_connection_pool(min_conn=1, max_conn=5):
    """
    Initialize the tracker database connection pool if it has not been created yet.

    Parameters
    ----------
    min_conn
        Minimum number of open connections kept in the pool.
    max_conn
        Maximum number of connections the pool will hand out at once.

    Return
    ----------
    psycopg2.pool.ThreadedConnectionPool
        The shared pool instance.
    """
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            os.environ["RUNNING_JOBS_DB_URL"],
        )
    return _connection_pool


def get_connection():
    """
    Borrow a connection from the pool, creating the pool on first use.

    Return
    ----------
    psycopg2.extensions.connection
        Live database connection from the pool.
    """
    init_connection_pool()
    return _connection_pool.getconn()


def return_connection(conn):
    """
    Hand a connection back to the pool so it can be reused.

    Parameters
    ----------
    conn
        Connection previously obtained from ``get_connection``.
    """
    _connection_pool.putconn(conn)


def add_job(fastq_name: str, job_type: str, command: str, demand_id: str, status: str = "SUBMITTED", batch_name_from_vendor: str = None):
    """
    Insert a new job row in ``running_jobs``, or update the existing row for this FASTQ and
    job type.

    Parameters
    ----------
    fastq_name
        Name of the FASTQ file associated with the job.
    job_type
        Job type, for example ``alignment`` or ``postqc``.
    command
        Command string that was submitted to OCS.
    demand_id
        OCS demand id returned by the submission.
    status
        Job status to record (defaults to ``SUBMITTED``).
    batch_name_from_vendor
        Optional vendor batch name to store alongside the job.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM running_jobs WHERE fastq_name = %s AND job_type = %s",
                   (fastq_name, job_type))
    existing = cursor.fetchone()

    if existing:
        cursor.execute("UPDATE running_jobs SET command = %s, demand_id = %s, status = %s, batch_name_from_vendor = %s, updated_at = NOW() WHERE fastq_name = %s AND job_type = %s",
                       (command, demand_id, status, batch_name_from_vendor, fastq_name, job_type))
    else:
        cursor.execute("INSERT INTO running_jobs (fastq_name, job_type, command, demand_id, status, batch_name_from_vendor) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                       (fastq_name, job_type, command, demand_id, status, batch_name_from_vendor))

    conn.commit()
    cursor.close()
    return_connection(conn)


def get_job(fastq_name: str, job_type: str) -> dict:
    """
    Look up a single job row by FASTQ name and job type.

    Parameters
    ----------
    fastq_name
        FASTQ name to look up.
    job_type
        Job type, for example ``alignment`` or ``postqc``.

    Return
    ----------
    dict or None
        Row as a dict if a matching job exists, otherwise ``None``.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT id, fastq_name, job_type, command, demand_id, status, batch_name_from_vendor, created_at, updated_at FROM running_jobs WHERE fastq_name = %s AND job_type = %s",
                   (fastq_name, job_type))
    result = cursor.fetchone()
    cursor.close()
    return_connection(conn)

    return result


def check_job_status(fastq_name: str, job_type: str) -> str:
    """
    Check for a job in the tracker database and, when found, refresh its status from OCS.

    When a matching job exists, this calls ``ocs fastqs <stage> get-status`` for the stored
    demand id and writes the current status back to ``running_jobs`` via ``update_job_status``.

    Parameters
    ----------
    fastq_name
        FASTQ name to look up.
    job_type
        Job type, ``alignment`` or ``postqc``.

    Return
    ----------
    str or None
        Latest job status reported by OCS, or ``None`` when the job is not tracked.
    """
    job = get_job(fastq_name, job_type)

    if not job:
        return None

    demand_id = job["demand_id"]
    endpoint_map = {"alignment": "align", "postqc": "postalign"}
    cmd = ["ocs", "fastqs", endpoint_map[job_type], "get-status", "--demand-id", demand_id, "--format", "json"]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)

    if not result.stdout.strip():
        return None

    status_data = json.loads(result.stdout)
    if not status_data:
        return None

    status = status_data[0].get("status")
    if status:
        update_job_status(fastq_name, job_type, status)
    return status


def update_job_status(fastq_name: str, job_type: str, status: str):
    """
    Update the status column (and ``updated_at``) of a tracked job.

    Parameters
    ----------
    fastq_name
        FASTQ name of the job to update.
    job_type
        Job type, for example ``alignment`` or ``postqc``.
    status
        New status value to write.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("UPDATE running_jobs SET status = %s, updated_at = NOW() WHERE fastq_name = %s AND job_type = %s",
                   (status, fastq_name, job_type))
    conn.commit()
    cursor.close()
    return_connection(conn)
