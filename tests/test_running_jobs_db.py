from unittest.mock import MagicMock, patch

from psycopg2 import OperationalError

from ocs_submission import running_jobs_db


def test_get_connection_returns_pooled_connection_when_alive():
    conn = MagicMock()
    connection_pool = MagicMock()
    connection_pool.getconn.return_value = conn

    with patch.object(running_jobs_db, "init_connection_pool", return_value=connection_pool):
        assert running_jobs_db.get_connection() is conn

    conn.rollback.assert_called_once_with()
    connection_pool.putconn.assert_not_called()


def test_get_connection_discards_server_closed_idle_connection():
    stale_conn = MagicMock()
    stale_conn.rollback.side_effect = OperationalError("SSL connection has been closed unexpectedly")
    fresh_conn = MagicMock()
    connection_pool = MagicMock()
    connection_pool.getconn.side_effect = [stale_conn, fresh_conn]

    with patch.object(running_jobs_db, "init_connection_pool", return_value=connection_pool):
        assert running_jobs_db.get_connection() is fresh_conn

    connection_pool.putconn.assert_called_once_with(stale_conn, close=True)
    assert connection_pool.getconn.call_count == 2
