"""tests/test_smoke.py - Smoke test for the example.

WHY: Professional Python projects include tests to verify that code runs
     correctly and to catch problems early when changes are made.
     Running tests is part of the standard workflow in every module.

OBS: You do not need to read or modify this file.
     It exists so that `uv run python -m pytest` passes.
"""

from streaming import (
    kafka_consumer_case,
    kafka_producer_case,
    kafka_consumer_nba,
    kafka_producer_nba,
)


def test_consumer_module_imports() -> None:
    """Consumer module should import without running Kafka operations."""
    assert kafka_consumer_case is not None


def test_producer_module_imports() -> None:
    """Producer module should import without running Kafka operations."""
    assert kafka_producer_case is not None

def test_nba_consumer_module_imports() -> None:
    """NBA Consumer module should import without running Kafka operations."""
    assert kafka_consumer_nba is not None

def test_nba_producer_module_imports() -> None:
    """NBA Producer module should import without running Kafka operations."""
    assert kafka_producer_nba is not None
