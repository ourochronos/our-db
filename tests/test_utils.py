"""Tests for our_db.utils."""

from our_db.utils import escape_ilike


class TestEscapeIlike:
    def test_plain_string(self):
        assert escape_ilike("hello") == "hello"

    def test_escapes_percent(self):
        assert escape_ilike("50%") == "50\\%"

    def test_escapes_underscore(self):
        assert escape_ilike("user_name") == "user\\_name"

    def test_escapes_backslash(self):
        assert escape_ilike("path\\to") == "path\\\\to"

    def test_escapes_all(self):
        assert escape_ilike("50%_test\\end") == "50\\%\\_test\\\\end"

    def test_empty_string(self):
        assert escape_ilike("") == ""

    def test_no_special_chars(self):
        assert escape_ilike("simple text") == "simple text"
