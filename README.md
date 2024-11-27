# db-toys
Toy database implementations for a deep study of *Designing Data-Intensive Applications* by Martin Kleppman.

## Implemented

### Log-structured storage engines

- Baseline in-memory, which stores a list of entries and searches linearly through them.
- Baseline with CSV-like IO, which appends entries to a file and loads the whole file into memory to search linearly through them.
- Indexed with random access-like IO, which keeps an index of offsets where each entry is written and uses it to retrieve them efficiently.

## Key findings

### List of single-key dictionaries vs single dictionary with multiple keys

~~Performance evaluations highlighted the memory efficiency of the baseline in-memory log-structured engine! It turns out 
that a list of single-key dictionary in Python is much more compact than a single dictionary with the same number of 
element -- less than twice the size, to be precise. The constant-time lookup that a single dictionary provides really 
comes at a cost, which is probably mostly impacted by the large hash table and collision-handling mechanisms.~~

Turns out that `sys.getsizeof()` was the wrong tool for this, since it only calculates the pointer overhead. So yes,
there is more pointer overhead in a large dictionary as opposed to a list of single-key dictionaries with the same 
contents; that is not unexpected. The total memory consumption, obtained with `pympler.asizeof()`, is about one third 
that of the baseline in-memory storage engine. It makes more sense that storing key-value pairs be less efficient than
storing key-offset pairs.


### Handling IO

The most realistic way to simulate a storage engine is to not close log files after each operation. This ensures the IO
operations, which introduce significant overhead, are kept to a minimum.

### A new interface is needed

My first interfaces were:

- Base storage engine, with get and set methods.
- Base IO handler mixin, with read and append to log methods.

Going beyond simple baseline engines, it became clear these are not sufficient.

1. I want to keep these toy DBs simple, but with a degree of realism. IO operations should mimic real implementations. At the very least, I should add open/close methods.
2. As the engines become more stateful, the problem arises where that state should be kept. This will be an evolving issue and whatever interfaces I come up with might not be fully generalizable to all DBs I will implement.

### Bitcask

An easy sanity check is to run Bitcask with only one segment and no compacting: it should have comparable memory 
consumption to our simple indexed log-structured engine. The write and read latency are also surprisingly similar, 
despite the higher overhead for Bitcask.
