from pathlib import Path
import time
import sys

from log_structured.baseline import BaselineLogStructuredStorageEngine
from log_structured.baseline_inmemory import BaselineInMemoryLogStructuredStorageEngine
from log_structured.indexed import IndexedLogStructuredStorageEngine

"""
Performance testing on each storage engine implemented.
Note that OOM errors are not tested, only basic memory usage at the end of the test for a quick comparison.
"""

# Define all storage engine classes to test
STORAGE_ENGINE_CLASSES = [
    BaselineLogStructuredStorageEngine,
    BaselineInMemoryLogStructuredStorageEngine,
    IndexedLogStructuredStorageEngine,
]

# Clean the test log file
TEST_LOG_PATH = "log.txt"
Path(TEST_LOG_PATH).unlink(missing_ok=True)


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
        pass


def object_size_in_kb(obj):
    """
    Returns the size of the given object in KB
    """
    size = sys.getsizeof(obj)
    return size / 1024


def run_tests_on_engine(engine_class, num_entries: int):
    print(f"\nTesting storage engine: {engine_class.__name__} on {num_entries} entries")
    engine = engine_class()

    # Run the performance tests
    write_performance(engine, num_entries)
    read_performance(engine, num_entries)
    worst_case_read_performance(engine, "non_existing_key")

    # Memory usage at end of test. The tests I'll run on these toy engines are small in the interest of time, so these
    # numbers should be evaluated within the context of performance comparison.
    # E.g., 50 KB is generally tiny, but significantly larger than 0.05 KB.
    memory_size = object_size_in_kb(engine.data)
    print(f"Memory usage: {memory_size:.2f} KB")


def main():
    # Configure test parameters
    num_entries = 10_000

    print(f"Found {len(STORAGE_ENGINE_CLASSES)} storage engines to test:")
    for engine_class in STORAGE_ENGINE_CLASSES:
        print(f" - {engine_class.__name__}")

    # Run tests
    for engine_class in STORAGE_ENGINE_CLASSES:
        run_tests_on_engine(engine_class, num_entries)

    # Cleanup
    Path(TEST_LOG_PATH).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
