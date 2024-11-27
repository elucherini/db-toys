from pathlib import Path
import time
import shutil
from typing import Dict, Any

from pympler import asizeof

from base.storage_engine import BaseStorageEngine
from log_structured.baseline import BaselineLogStructuredStorageEngine
from log_structured.baseline_inmemory import BaselineInMemoryLogStructuredStorageEngine
from log_structured.indexed import IndexedLogStructuredStorageEngine
from log_structured.bitcask import Bitcask

"""
Performance testing on each storage engine implemented.
Note that OOM errors are not tested, only basic memory usage at the end of the test for a quick comparison.
"""

# Define all storage engine classes to test
STORAGE_ENGINE_CLASSES = [
    (BaselineLogStructuredStorageEngine, {}),
    (BaselineInMemoryLogStructuredStorageEngine, {}),
    (IndexedLogStructuredStorageEngine, {}),
    (Bitcask, {"max_segment_size": 1024 * 1024}),
]

# Clean the test log file
TEST_LOG_PATH = "log.txt"
PARENT_DIRECTORY = "perftests"


def measure_time(func):
    """
    A decorator to measure the execution time of a function.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"{func.__name__} took {elapsed_time:.6f} seconds")
        return result
    return wrapper


def teardown_dir(dir_path: Path):
    if dir_path.exists() and dir_path.is_dir():
        # Delete the directory and all its contents
        shutil.rmtree(dir_path)


def cleanup(dir_path: str = PARENT_DIRECTORY, file_path: str = TEST_LOG_PATH):
    # Split the file name into the base name and extension
    base_name, extension = file_path.rsplit(".", 1)

    # Construct the pattern
    pattern = f"{base_name}*.{extension}"

    # Find and delete matching files
    for file_path in Path(dir_path).glob(pattern):
        if file_path.is_file():  # Ensure it's a file
            file_path.unlink()


@measure_time
def write_performance(engine, num_entries: int):
    """
    Test the write performance by writing a large number of entries.
    """
    for i in range(num_entries):
        engine.set(f"key_{i}", f"value_{i}")


@measure_time
def read_performance(engine, num_entries: int):
    """
    Test the read performance by retrieving values for all keys.
    """
    for i in range(num_entries):
        assert engine.get(f"key_{i}") == f"value_{i}"


@measure_time
def worst_case_read_performance(engine, non_existing_key: str):
    """
    Test the performance of searching for a non-existing key (worst-case scenario).
    """
    try:
        result = engine.get(non_existing_key)
        assert result is None, f"Expected None, but got {result}"
    except ValueError:
        print(f"threw exception for nonexistent key")


def object_size_in_kb(obj):
    """
    Returns the size of the given object in KB
    """
    size = asizeof.asizeof(obj)
    return size / 1024


def run_tests_on_engine(engine_class: BaseStorageEngine, init_params: Dict[str, Any], num_entries: int):
    print(f"\nTesting storage engine: {engine_class.__name__} with parameters {init_params}")
    engine = engine_class(**init_params)

    # Run the performance tests
    write_performance(engine, num_entries)
    read_performance(engine, num_entries)
    worst_case_read_performance(engine, "non_existing_key")

    # Memory usage at end of test. The tests I'll run on these toy engines are small in the interest of time, so these
    # numbers should be evaluated within the context of performance comparison.
    # E.g., 50 KB is generally tiny, but significantly larger than 0.05 KB.
    memory_size = object_size_in_kb(engine.data)
    print(f"Memory usage: {memory_size:.2f} KB")

    cleanup()


def main():
    # Configure test parameters
    num_entries = 10_000

    teardown_dir(Path(PARENT_DIRECTORY))

    # Set up test directory
    Path(PARENT_DIRECTORY).mkdir(parents=True, exist_ok=True)
    db_path = Path(PARENT_DIRECTORY) / TEST_LOG_PATH

    print(f"Found {len(STORAGE_ENGINE_CLASSES)} storage engines to test:")
    for engine_class, params in STORAGE_ENGINE_CLASSES:
        print(f" - {engine_class.__name__} with parameters {params}")

    # Run tests
    for engine_class, params in STORAGE_ENGINE_CLASSES:
        # Inject test path
        params["path"] = db_path
        run_tests_on_engine(engine_class, params, num_entries)

    teardown_dir(Path(PARENT_DIRECTORY))


if __name__ == "__main__":
    main()
