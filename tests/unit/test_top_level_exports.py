"""Everything in ``reflowfy.__all__`` resolves, and the lazy names load only on use."""

import subprocess
import sys

import pytest

import reflowfy


def test_every_exported_name_resolves() -> None:
    missing = [name for name in reflowfy.__all__ if getattr(reflowfy, name, None) is None]
    assert missing == []
    assert len(reflowfy.__all__) == len(set(reflowfy.__all__))


def test_new_exports_are_the_real_objects() -> None:
    from reflowfy.core.job_retry import JobRetryConfig
    from reflowfy.core.secrets import secret
    from reflowfy.destinations.base import PermanentDestinationError, RetryConfig
    from reflowfy.destinations.fan_out import fan_out
    from reflowfy.reflow_manager.lifecycle import run_pipeline
    from reflowfy.sources.s3 import s3_source

    assert reflowfy.JobRetryConfig is JobRetryConfig
    assert reflowfy.secret is secret
    assert reflowfy.RetryConfig is RetryConfig
    assert reflowfy.PermanentDestinationError is PermanentDestinationError
    assert reflowfy.fan_out is fan_out
    assert reflowfy.run_pipeline is run_pipeline
    assert reflowfy.s3_source is s3_source


def test_lazy_names_are_not_imported_with_the_package() -> None:
    # run_pipeline pulls in the manager's database layer; a worker never needs it.
    code = "import sys, reflowfy; print('reflowfy.reflow_manager.lifecycle' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


def test_unknown_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="no attribute 'nope'"):
        getattr(reflowfy, "nope")


def test_s3_source_without_boto3_says_how_to_install(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    real = importlib.import_module

    def fake(name: str, package: str | None = None):  # type: ignore[no-untyped-def]
        if name == "reflowfy.sources.s3":
            raise ImportError("No module named 'boto3'")
        return real(name, package)

    monkeypatch.delitem(reflowfy.__dict__, "s3_source", raising=False)
    monkeypatch.setattr(importlib, "import_module", fake)
    with pytest.raises(ImportError, match="pip install boto3"):
        getattr(reflowfy, "s3_source")


def test_other_import_errors_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    def fake(name: str, package: str | None = None):  # type: ignore[no-untyped-def]
        raise ImportError("broken")

    monkeypatch.delitem(reflowfy.__dict__, "run_pipeline", raising=False)
    monkeypatch.setattr(importlib, "import_module", fake)
    with pytest.raises(ImportError, match="^broken$"):
        getattr(reflowfy, "run_pipeline")
