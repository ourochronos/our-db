"""Smoke tests for our-db package."""

from our_db import __version__


def test_version_is_string():
    assert isinstance(__version__, str)


def test_version_is_semver():
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
