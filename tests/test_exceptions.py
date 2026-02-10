"""Tests for our_db.exceptions."""

import pytest

from our_db.exceptions import (
    ConfigError,
    ConflictError,
    DatabaseError,
    NotFoundError,
    OroDbError,
    ValidationError,
)


class TestOroDbError:
    def test_basic_creation(self):
        exc = OroDbError("test error")
        assert exc.message == "test error"
        assert exc.details == {}
        assert str(exc) == "test error"

    def test_with_details(self):
        exc = OroDbError("test error", details={"key": "value"})
        assert exc.details == {"key": "value"}

    def test_to_dict(self):
        exc = OroDbError("test error", details={"key": "value"})
        result = exc.to_dict()
        assert result == {
            "error": "OroDbError",
            "message": "test error",
            "details": {"key": "value"},
        }

    def test_is_exception(self):
        exc = OroDbError("test")
        assert isinstance(exc, Exception)


class TestDatabaseError:
    def test_inherits_base(self):
        exc = DatabaseError("db error")
        assert isinstance(exc, OroDbError)
        assert exc.message == "db error"

    def test_to_dict_uses_class_name(self):
        exc = DatabaseError("db error")
        assert exc.to_dict()["error"] == "DatabaseError"


class TestValidationError:
    def test_basic(self):
        exc = ValidationError("invalid input")
        assert exc.message == "invalid input"
        assert exc.field is None
        assert exc.value is None

    def test_with_field_and_value(self):
        exc = ValidationError("bad value", field="name", value=42)
        assert exc.field == "name"
        assert exc.value == 42
        assert exc.details == {"field": "name", "value": "42"}

    def test_inherits_base(self):
        assert isinstance(ValidationError("x"), OroDbError)


class TestConfigError:
    def test_basic(self):
        exc = ConfigError("config error")
        assert exc.message == "config error"
        assert exc.missing_vars == []

    def test_with_missing_vars(self):
        exc = ConfigError("missing config", missing_vars=["DB_HOST", "DB_PORT"])
        assert exc.missing_vars == ["DB_HOST", "DB_PORT"]
        assert exc.details == {"missing_vars": ["DB_HOST", "DB_PORT"]}


class TestNotFoundError:
    def test_creates_message(self):
        exc = NotFoundError("Belief", "abc-123")
        assert exc.message == "Belief not found: abc-123"
        assert exc.resource_type == "Belief"
        assert exc.resource_id == "abc-123"

    def test_to_dict(self):
        exc = NotFoundError("Entity", "xyz")
        result = exc.to_dict()
        assert result["details"]["resource_type"] == "Entity"
        assert result["details"]["resource_id"] == "xyz"


class TestConflictError:
    def test_basic(self):
        exc = ConflictError("duplicate")
        assert exc.message == "duplicate"
        assert exc.existing_id is None

    def test_with_existing_id(self):
        exc = ConflictError("duplicate", existing_id="abc-123")
        assert exc.existing_id == "abc-123"
        assert exc.details == {"existing_id": "abc-123"}


def test_exception_hierarchy():
    """All exceptions should be catchable as OroDbError."""
    exceptions = [
        DatabaseError("db"),
        ValidationError("val"),
        ConfigError("cfg"),
        NotFoundError("type", "id"),
        ConflictError("dup"),
    ]
    for exc in exceptions:
        assert isinstance(exc, OroDbError)
        with pytest.raises(OroDbError):
            raise exc
