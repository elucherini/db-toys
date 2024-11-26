import pytest
from pathlib import Path
from log_structured.baseline import BaselineLogStructuredStorageEngine
from log_structured.baseline_inmemory import BaselineInMemoryLogStructuredStorageEngine
from log_structured.indexed import IndexedLogStructuredStorageEngine


@pytest.fixture
def storage_engine(request):
    """
    A fixture to create instances of the storage engine class under test.

    Usage:
        Add `storage_engine` as a parameter to your test and use `request` to
        pass the subclass when invoking pytest.
    """
    storage_cls = request.param
    return storage_cls()


@pytest.mark.parametrize(
    "storage_engine",
    [
        pytest.param(lambda: BaselineLogStructuredStorageEngine(), id="BaselineLogStructuredStorageEngine"),
        pytest.param(lambda: BaselineInMemoryLogStructuredStorageEngine(),
                     id="BaselineInMemoryLogStructuredStorageEngine"),
        pytest.param(lambda: IndexedLogStructuredStorageEngine(), id="IndexedLogStructuredStorageEngine")
    ],
    indirect=True,
)
def test_storage_engine(storage_engine):
    """
    Generic test for any BaseStorageEngine subclass.
    """
    # Test set and get functionality
    storage_engine.set("42", "{example example}")
    storage_engine.set("10", "{another example}")
    assert storage_engine.get("42") == "{example example}"
    assert storage_engine.get("10") == "{another example}"

    # Test update functionality
    storage_engine.set("42", "{updated}")
    assert storage_engine.get("42") == "{updated}"
