# Task List

Rough Timeline   | Tasks
----------------|----------------------------------------
February 12, 2024 | Pair Finalisation, Complete Code walk through
February 26, 2024 | Define class for data records
March 11, 2024    | Add predicate evaluation to FilterIterator
March 25, 2024    | Add in-memory sorting, duplicate removal
April 8, 2024     | Add Plan & Iterator that verify a set of rows, Performance Testing and optimization
April 22, 2024    | Performance Testing and optimization
April 29, 2024    | Submission

## Milestones

...
Trace existing code
Disable (not remove!) excessive tracing output
Define class for data records
Add data records (incl mem mgmt) to iterators
Add data generation (random values) in ScanIterator
Test with simple plan -- scan only
Add predicate evaluation to FilterIterator
Test with moderate plan -- scan & filter
Add in-memory sorting -- eg 10, 100, 1000 rows
Add duplicate removal  -- eg. sort and eliminate duplicates
Test with moderate plan -- scan & sort
Add Plan & Iterator that verify a sort order
Test with 0 rows and with 1 row, also 2, 3, 7 rows
Add Plan & Iterator that verify a set of rows
Test with 0, 1, 2, 3, 10, 29, 100, 576, 1000 rows
...

test with inputs of 50 MB, 125 MB, 12 GB, and 120 GB
record sizes 20Bytes to 2000 Bytes
●
1 CPU core,
1 MB cache, (1048576 bytes)
100 MB DRAM
SSD: 10 GB capacity, 0.1 ms latency, 200 MB/s bandwidth
HDD: ∞ capacity, 5 ms latency, 100 MB/s bandwidth
Emulate SSD + HDD, report total latency & transfer time

●
Extra credit: logic & performance evaluation for
    ○ in-stream (after-sort) ‘distinct’, ‘group by’, or ‘top’
    ○ in-sort ‘distinct’, ‘group by’, or ‘top’

● Provided: iterator template & logic

Efficient random data generation
<https://stackoverflow.com/questions/25298585/efficiently-generating-random-bytes-of-data-in-c11-14>

## Sample I/O

Hello Everyone,
I have uploaded sample input, output, and trace file in the media folder.
Let me know if you have any problem accessing the same.
There 20 records and each of size 1024 bytes.
Regarding how to test your project: you can use the CSL machine the department
has and also use Docker to test out. Docker provides flexibility with memory
but the hiccup is to write a Dockerfile for your project.
Thank You
1] Input arguements when generating the trace0.txt was:
20 records and each of size 1024 bytes

`./ExternalSort.exe -c 20 -s 1024 -o trace0.txt`
CACHE_SIZE      `1 * 1024 * 1024`         // 1 MB
DRAM_SIZE       `100 * 1024 * 1024`       // 100 MB
SSD_SIZE        `10 * 1024 * 1024 * 1024` // 10 GB
SSD_LATENCY     `1 / (10 * 1000)`         // 0.1 ms
SSD_BANDWIDTH   `100 * 1024 * 1024`       // 100 MB/s
HDD_SIZE        `INT_MAX`                 // Infinite
HDD_LATENCY     `1 * 10 / 1000`           // 10 ms
HDD_BANDWIDTH   `100 * 1024 * 1024`       // 100 MB/s

These are the specifications it was run with, almost the same as the current project but slightly different.

The algorithm is adjusted dynamically to data written to the SSD based on the memory resources. Pleases remeber that the trace was generated with a small number of records.

Each states and accesses in the log are representing different pahses of the sorting process, including merging sorted runs, reading the data and spilling the sorted runs.

2]
You can estimate the size of the output buffer page in DRAM based on system-level latency and bandwidth metrics. You cannot use the SSD latency and bandwidth for DRAM.

If your system has a measured average memory access latency of, say, 100 nanoseconds (ns) and a memory bandwidth of 100 GB/s (gigabytes per second), you can estimate the buffer page size: 
Page Size ≈ 100 ns×100 GB/s
            =100 GB×1001,000,000
            =10 kilobytes (KB)