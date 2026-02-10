"""Tests for our_db.migrations."""

from unittest.mock import MagicMock

import pytest

from our_db.migrations import MigrationRunner


class TestMigrationRunner:
    def test_init_with_path(self, tmp_path):
        runner = MigrationRunner(migrations_dir=tmp_path)
        assert runner.migrations_dir == tmp_path

    def test_init_requires_migrations_dir(self):
        with pytest.raises(TypeError):
            MigrationRunner()  # type: ignore[call-arg]


class TestDiscover:
    def test_empty_dir(self, tmp_path):
        runner = MigrationRunner(migrations_dir=tmp_path)
        assert runner.discover() == []

    def test_missing_dir(self, tmp_path):
        runner = MigrationRunner(migrations_dir=tmp_path / "nonexistent")
        assert runner.discover() == []

    def test_discovers_migrations(self, tmp_path):
        migration = tmp_path / "001_create_table.py"
        migration.write_text("""
version = "001"
description = "Create table"

def up(conn) -> None:
    pass

def down(conn) -> None:
    pass
""")
        runner = MigrationRunner(migrations_dir=tmp_path)
        migrations = runner.discover()
        assert len(migrations) == 1
        assert migrations[0].version == "001"
        assert migrations[0].description == "Create table"

    def test_skips_dunder_files(self, tmp_path):
        (tmp_path / "__init__.py").write_text("")
        runner = MigrationRunner(migrations_dir=tmp_path)
        assert runner.discover() == []

    def test_skips_non_migration_files(self, tmp_path):
        (tmp_path / "helper.py").write_text("# not a migration")
        runner = MigrationRunner(migrations_dir=tmp_path)
        assert runner.discover() == []

    def test_sorts_by_version(self, tmp_path):
        for num in [3, 1, 2]:
            p = tmp_path / f"00{num}_test.py"
            p.write_text(f"""
version = "00{num}"
description = "Migration {num}"

def up(conn) -> None:
    pass

def down(conn) -> None:
    pass
""")
        runner = MigrationRunner(migrations_dir=tmp_path)
        versions = [m.version for m in runner.discover()]
        assert versions == ["001", "002", "003"]

    def test_validates_required_attributes(self, tmp_path):
        migration = tmp_path / "001_bad.py"
        migration.write_text('version = "001"\n')  # missing description, up, down

        runner = MigrationRunner(migrations_dir=tmp_path)
        with pytest.raises(ValueError, match="missing required attribute"):
            runner.discover()

    def test_caches_results(self, tmp_path):
        migration = tmp_path / "001_test.py"
        migration.write_text("""
version = "001"
description = "Test"

def up(conn) -> None:
    pass

def down(conn) -> None:
    pass
""")
        runner = MigrationRunner(migrations_dir=tmp_path)
        result1 = runner.discover()
        result2 = runner.discover()
        assert result1 is result2

    def test_invalidate_cache(self, tmp_path):
        runner = MigrationRunner(migrations_dir=tmp_path)
        runner.discover()
        runner.invalidate_cache()
        assert runner._migrations is None


class TestComputeChecksum:
    def test_deterministic(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("content")
        cs1 = MigrationRunner._compute_checksum(f)
        cs2 = MigrationRunner._compute_checksum(f)
        assert cs1 == cs2
        assert len(cs1) == 16

    def test_different_content_different_checksum(self, tmp_path):
        f1 = tmp_path / "a.py"
        f1.write_text("content a")
        f2 = tmp_path / "b.py"
        f2.write_text("content b")
        assert MigrationRunner._compute_checksum(f1) != MigrationRunner._compute_checksum(f2)


class TestCreateMigration:
    def test_creates_file(self, tmp_path):
        path = MigrationRunner.create_migration(tmp_path, "add users table")
        assert path.exists()
        assert path.name == "001_add_users_table.py"
        content = path.read_text()
        assert 'version = "001"' in content
        assert 'description = "add users table"' in content
        assert "def up(conn)" in content
        assert "def down(conn)" in content

    def test_increments_version(self, tmp_path):
        MigrationRunner.create_migration(tmp_path, "first")
        path = MigrationRunner.create_migration(tmp_path, "second")
        assert path.name == "002_second.py"

    def test_creates_directory(self, tmp_path):
        new_dir = tmp_path / "migrations"
        path = MigrationRunner.create_migration(new_dir, "init")
        assert new_dir.is_dir()
        assert path.exists()


class TestConnectionFactory:
    def test_custom_factory(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)

        # Status should use the custom factory
        runner.status()
        factory.assert_called()

    def test_custom_factory_closes_on_put(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)
        runner.status()
        mock_conn.close.assert_called()


class TestUp:
    def _make_runner_with_migration(self, tmp_path):
        migration = tmp_path / "001_test.py"
        migration.write_text("""
version = "001"
description = "Test"

def up(conn) -> None:
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.close()

def down(conn) -> None:
    pass
""")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)
        return runner, mock_conn

    def test_applies_pending(self, tmp_path):
        runner, mock_conn = self._make_runner_with_migration(tmp_path)
        applied = runner.up()
        assert applied == ["001"]
        mock_conn.commit.assert_called()

    def test_dry_run(self, tmp_path):
        runner, mock_conn = self._make_runner_with_migration(tmp_path)
        applied = runner.up(dry_run=True)
        assert applied == ["001"]
        # Dry run should not call commit for the migration itself
        # (only for _ensure_table)

    def test_no_pending(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # Simulate migration already applied
        mock_cursor.fetchall.return_value = [
            {"version": "001", "description": "Test", "checksum": "abc", "applied_at": "2024-01-01"}
        ]

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)
        # Empty dir = no migrations to discover
        applied = runner.up()
        assert applied == []


class TestDown:
    def test_no_applied(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)
        rolled_back = runner.down()
        assert rolled_back == []


class TestBootstrap:
    def test_fails_if_already_applied(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {"version": "001", "description": "Test", "checksum": "abc", "applied_at": "2024-01-01"}
        ]

        factory = MagicMock(return_value=mock_conn)
        runner = MigrationRunner(migrations_dir=tmp_path, connection_factory=factory)

        with pytest.raises(RuntimeError, match="Cannot bootstrap"):
            runner.bootstrap()
