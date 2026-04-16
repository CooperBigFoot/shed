"""Smoke tests for the pyshed Python bindings."""


def test_import():
    """Module can be imported."""
    import pyshed

    assert hasattr(pyshed, "Engine")


def test_exceptions_exist():
    """Custom exception classes are importable."""
    from pyshed import DatasetError, ResolutionError, ShedError

    assert issubclass(DatasetError, ShedError)
    assert issubclass(ResolutionError, ShedError)


def test_engine_bad_path():
    """Engine raises DatasetError for a nonexistent path."""
    import pytest

    import pyshed

    with pytest.raises(pyshed.DatasetError):
        pyshed.Engine("/nonexistent/path/to/dataset")
