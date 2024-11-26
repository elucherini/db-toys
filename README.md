# db-toys
Toy database implementations for a deep study of *Designing Data-Intensive Applications* by Martin Kleppman.

## Implemented

### Log-structured storage engines

- Baseline in-memory, which stores a list of entries and searches linearly through them.
- Baseline with CSV-like IO, which appends entries to a file and loads the whole file into memory to search linearly through them.
- Indexed with random access-like IO, which keeps an index of offsets where each entry is written and uses it to retrieve them efficiently.

## Key findings

### List of single-key dictionaries vs single dictionary with multiple keys

Performance evaluations highlighted the memory efficiency of the baseline in-memory log-structured engine! It turns out 
that a list of single-key dictionary in Python is much more compact than a single dictionary with the same number of 
element -- less than twice the size, to be precise. The constant-time lookup that a single dictionary provides really 
comes at a cost, which is probably mostly impacted by the large hash table and collision-handling mechanisms.
