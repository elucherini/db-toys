from pathlib import Path
import time
from log_structured.baseline import BaselineLogStructuredStorageEngine
from log_structured.baseline_inmemory import BaselineInMemoryLogStructuredStorageEngine


# Clean the test log file
TEST_LOG_PATH = "performance_log.txt"
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
def test_write_performance(engine, num_entries: int):
    """
    Test the write performance by writing a large number of entries.
    """
    for i in range(num_entries):
        engine.set(f"key_{i}", f"value_{i}")


@measure_time
def test_read_performance(engine, num_entries: int):
    """
    Test the read performance by retrieving values for all keys.
    """
    for i in range(num_entries):
        assert engine.get(f"key_{i}") == f"value_{i}"


@measure_time
def test_worst_case_read_performance(engine, non_existing_key: str):
    """
    Test the performance of searching for a non-existing key (worst-case scenario).
    """
    result = engine.get(non_existing_key)
    assert result is None, f"Expected None, but got {result}"


def run_tests_on_engine(engine_class, num_entries: int):
    print(f"\nTesting storage engine: {engine_class.__name__}")
    engine = engine_class()

    # Run the performance tests
    test_write_performance(engine, num_entries)
    test_read_performance(engine, num_entries)
    test_worst_case_read_performance(engine, "non_existing_key")



def main():
    # Define all storage engine classes to test
    storage_engine_classes = [
        BaselineLogStructuredStorageEngine,
        BaselineInMemoryLogStructuredStorageEngine,
    ]

    # Configure test parameters
    num_entries = 10_000

    print(f"Found {len(storage_engine_classes)} storage engines to test:")
    for engine_class in storage_engine_classes:
        print(f" - {engine_class.__name__}")

    # Run tests
    for engine_class in storage_engine_classes:
        run_tests_on_engine(engine_class, num_entries)

    # Cleanup
    Path(TEST_LOG_PATH).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
