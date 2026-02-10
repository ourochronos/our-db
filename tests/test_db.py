"""Tests for our_db.db.

These tests mock psycopg2 to avoid requiring a real database connection.
"""

from unittest.mock import MagicMock, patch

import pytest

from our_db.db import (
    ConnectionPool,
    generate_id,
    get_connection_params,
    get_pool_config,
)
from our_db.exceptions import DatabaseError


class TestGenerateId:
    def test_returns_string(self):
        result = generate_id()
        assert isinstance(result, str)

    def test_returns_uuid_format(self):
        result = generate_id()
        parts = result.split("-")
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12

    def test_returns_unique_values(self):
        ids = {generate_id() for _ in range(100)}
        assert len(ids) == 100


class TestGetConnectionParams:
    def test_returns_dict(self):
        params = get_connection_params()
        assert isinstance(params, dict)
        assert "host" in params
        assert "port" in params
        assert "dbname" in params
        assert "user" in params
        assert "password" in params


class TestGetPoolConfig:
    def test_returns_dict(self):
        config = get_pool_config()
        assert isinstance(config, dict)
        assert "minconn" in config
        assert "maxconn" in config
        assert isinstance(config["minconn"], int)
        assert isinstance(config["maxconn"], int)


class TestConnectionPool:
    def setup_method(self):
        """Reset the singleton before each test."""
        ConnectionPool._instance = None

    def test_get_instance_returns_singleton(self):
        pool1 = ConnectionPool.get_instance()
        pool2 = ConnectionPool.get_instance()
        assert pool1 is pool2

    def test_reset_instance(self):
        pool1 = ConnectionPool.get_instance()
        ConnectionPool.reset_instance()
        pool2 = ConnectionPool.get_instance()
        assert pool1 is not pool2

    def test_get_stats_uninitialized(self):
        pool = ConnectionPool()
        stats = pool.get_stats()
        assert stats["initialized"] is False
        assert "min_connections" in stats
        assert "max_connections" in stats

    @patch("our_db.db.psycopg2_pool.ThreadedConnectionPool")
    def test_get_connection_creates_pool(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool

        pool = ConnectionPool()
        conn = pool.get_connection()
        assert conn is mock_conn
        mock_pool_cls.assert_called_once()

    @patch("our_db.db.psycopg2_pool.ThreadedConnectionPool")
    def test_put_connection(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool

        pool = ConnectionPool()
        pool.get_connection()  # force pool init
        pool.put_connection(mock_conn)
        mock_pool.putconn.assert_called_once_with(mock_conn)

    @patch("our_db.db.psycopg2_pool.ThreadedConnectionPool")
    def test_close_all(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = MagicMock()
        mock_pool_cls.return_value = mock_pool

        pool = ConnectionPool()
        pool.get_connection()  # force pool init
        pool.close_all()
        mock_pool.closeall.assert_called_once()

    @patch("our_db.db.psycopg2_pool.ThreadedConnectionPool")
    def test_get_stats_initialized(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = MagicMock()
        mock_pool_cls.return_value = mock_pool

        pool = ConnectionPool()
        pool.get_connection()  # force pool init
        stats = pool.get_stats()
        assert stats["initialized"] is True

    @patch("our_db.db.psycopg2_pool.ThreadedConnectionPool")
    def test_get_connection_none_raises(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_pool.getconn.return_value = None
        mock_pool_cls.return_value = mock_pool

        pool = ConnectionPool()
        with pytest.raises(DatabaseError, match="pool exhausted"):
            pool.get_connection()


class TestGetCursor:
    @patch("our_db.db._pool")
    def test_get_cursor_yields_cursor(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import get_cursor

        with get_cursor() as cur:
            assert cur is mock_cursor

        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_pool.put_connection.assert_called_once_with(mock_conn)

    @patch("our_db.db._pool")
    def test_get_cursor_no_dict(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import get_cursor

        with get_cursor(dict_cursor=False) as cur:
            assert cur is mock_cursor

        mock_conn.cursor.assert_called_once_with(cursor_factory=None)


class TestGetConnectionContext:
    @patch("our_db.db._pool")
    def test_yields_connection(self, mock_pool):
        mock_conn = MagicMock()
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import get_connection_context

        with get_connection_context() as conn:
            assert conn is mock_conn

        mock_conn.commit.assert_called_once()
        mock_pool.put_connection.assert_called_once_with(mock_conn)


class TestInitSchema:
    @patch("our_db.db._pool")
    def test_init_schema_reads_files(self, mock_pool, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "schema.sql").write_text("CREATE TABLE test (id INT);")

        from our_db.db import init_schema

        init_schema(schema_dir)
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()

    @patch("our_db.db._pool")
    def test_init_schema_custom_files(self, mock_pool, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "custom.sql").write_text("SELECT 1;")

        from our_db.db import init_schema

        init_schema(schema_dir, schema_files=["custom.sql"])
        mock_cursor.execute.assert_called()

    @patch("our_db.db._pool")
    def test_init_schema_skips_missing_files(self, mock_pool, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        # No files created

        from our_db.db import init_schema

        init_schema(schema_dir)
        mock_cursor.execute.assert_not_called()
        mock_conn.commit.assert_called()


class TestCountRows:
    @patch("our_db.db._pool")
    def test_count_rows_with_allowlist(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [{"table_name": "users"}, {"count": 42}]
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import count_rows

        result = count_rows("users", valid_tables=frozenset(["users", "orders"]))
        assert result == 42

    @patch("our_db.db._pool")
    def test_count_rows_rejects_unlisted_table(self, mock_pool):
        from our_db.db import count_rows

        with pytest.raises(ValueError, match="not in allowlist"):
            count_rows("secret_table", valid_tables=frozenset(["users"]))

    @patch("our_db.db._pool")
    def test_count_rows_no_allowlist(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [{"table_name": "anything"}, {"count": 10}]
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import count_rows

        result = count_rows("anything")
        assert result == 10

    @patch("our_db.db._pool")
    def test_count_rows_nonexistent_table(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import count_rows

        with pytest.raises(ValueError, match="does not exist"):
            count_rows("nonexistent")


class TestCheckConnection:
    @patch("our_db.db._pool")
    def test_check_connection_success(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import check_connection

        assert check_connection() is True

    @patch("our_db.db._pool")
    def test_check_connection_failure(self, mock_pool):
        mock_pool.get_connection.side_effect = DatabaseError("connection failed")

        from our_db.db import check_connection

        assert check_connection() is False


class TestTableExists:
    @patch("our_db.db._pool")
    def test_table_exists_true(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {"exists": True}
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import table_exists

        assert table_exists("my_table") is True

    @patch("our_db.db._pool")
    def test_table_exists_false(self, mock_pool):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {"exists": False}
        mock_pool.get_connection.return_value = mock_conn

        from our_db.db import table_exists

        assert table_exists("nonexistent") is False
