import shutil

import pytest
from pathlib import Path
from log_structured.baseline import BaselineLogStructuredStorageEngine
from log_structured.baseline_inmemory import BaselineInMemoryLogStructuredStorageEngine
from log_structured.indexed import IndexedLogStructuredStorageEngine
from log_structured.bitcask import Bitcask


TEST_DIR = "testfiles"


@pytest.fixture(scope="function", autouse=True)
def clean_testfiles():
    """
    Fixture to ensure the test directory is clean before and after each test.
    """
    test_dir = Path(TEST_DIR)
    # Teardown: Ensure the directory is deleted before the test starts
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True)

    yield  # Run the test

    # Cleanup: Delete the directory after the test completes
    if test_dir.exists():
        shutil.rmtree(TEST_DIR)


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
        pytest.param(lambda: BaselineLogStructuredStorageEngine(TEST_DIR),
                     id="BaselineLogStructuredStorageEngine"),
        pytest.param(lambda: BaselineInMemoryLogStructuredStorageEngine(),
                     id="BaselineInMemoryLogStructuredStorageEngine"),
        pytest.param(lambda: IndexedLogStructuredStorageEngine(TEST_DIR),
                     id="IndexedLogStructuredStorageEngine"),
        # Bitcask with one segment
        pytest.param(lambda: Bitcask(TEST_DIR),
                     id="Bitcask_1segment"),
        # Bitcask with multiple segments
        pytest.param(lambda: Bitcask(TEST_DIR, max_segment_size=1),
                     id="Bitcask_Ksegments")
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
