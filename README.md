# CS764 External Merge Sort

## Setup and Run

Build the project using `make`.
It will create the executable `ExternalSort.exe`

### Command Line Arguments:

- `-c <num_records>`: Specifies the number of records to sort.
- `-s <record_size>`: Defines the size of each record in bytes.
- `-o <trace_file>`: Sets the name of the file where the traces of the program will be written. The default is `trace.log`.
- `-v`: [Optional] Enables verification of the sorted output. Checks both the order and the integrity, i.e., all records are present and how many duplicates are removed. 
- `-vo`: [Optional] This option skips the sorting process and only checks if the existing output file is sorted correctly. This option expects the input and output file are present in the current directory.

### Usage Examples

Here are some examples of how to run `ExternalSort.exe` with different configurations:

#### Sorting
```sh
./ExternalSort.exe -c 20 -s 1024 -o trace.txt
```
#### Sorting with verification:
```sh
./ExternalSort.exe -c 20 -s 1024 -o trace.txt -v
```
The `trace.txt` will containt the result of verification at the end of the file.

#### Verify only
```sh
./ExternalSort.exe -c 20 -s 1024 -o trace.txt -vo
```

### Output Files

This code will generate an input file `input-c20-s1024.txt` and an output file `output-c20-s1024.txt`.

The trace file will contain information about disk access, how many inputs have been sorted, how many duplicates have been removed and program duration.

Following is a snippet of `trace.log` file for `./ExternalSort.exe -c 11000000 -s 1024 -v`
```
...
========== INPUT_GEN START ========
Generated 11000000 records (5496832 of them are duplicate) in input-c11000000-s1024.txt
======= INPUT_GEN COMPLETE ========
Input_Gen Duration 43 seconds / 0 minutes
...
========= EXTERNAL_MERGE_SORT START =========
		STATE -> LOAD_INPUT: 102400 input records
		ACCESS -> A read from HDD was made with size 104857600 bytes and latency 1005.00 ms
	GEN_MINIRUNS START
	Sorted 102400 records and generated 100 miniruns
		STATE -> 2 cache-sized miniruns Spill to SSD
		ACCESS -> A write to SSD was made with size 2097152 bytes and latency 0.10 ms
		STATE -> Merging 98 cache-sized miniruns
		ACCESS -> A write to SSD was made with size 1056768 bytes and latency 0.10 ms
...
STATE -> SSD is full, Spill to HDD 4880640 records
		ACCESS -> A write to HDD was made with size 4997775360 bytes and latency 47005.00 ms
		STATE -> Merging runs, Spill to SSD_runs/r322.txt, 1920 records 
		ACCESS -> A write to SSD was made with size 1966080 bytes and latency 0.10 ms
...
============= FIRST_PASS COMPLETE ===========
First_Pass Duration: 31 seconds / 0 minutes
	MERGE_ITR 0: 0 runfiles in SSD, 1 runfiles in HDD
SUCCESS: all runs merged
======== EXTERNAL_MERGE_SORT COMPLETE =========
External_Merge_Sort Total Duration 34 seconds / 0 minutes
Removed 5496832 duplicate records out of 5496832 duplicates
===============================================
Cleanup done
============= Verifying order =============
Finished reading output
SUCCESS: Order verified with 5503168 records
============= Order verification successful =============
============= Verifying integrity =============
...
Total input records generated: 11000000
Total input records verified: 11000000
Total output records verified: 5503168
Duplicates removed: 5496832
SUCCESS: Integrity verified
============= Integrity verification successful =============
```

### Configuration

The capactity, bandwidth, and latency of different devices are kept in a `Config` class which is inside `Config.h`.

## Implemented Techniques

### Quicksort
We use quicksort for sorting cache-size inputs. The function `quickSort()` is in `StorageTypes.cpp:Line630:660`.

### Cache Size Mini Runs
We generate cache-size mini runs on the input loaded to memory. This is inside `genMiniRuns()` function in `StorageTypes.cpp:Line770-800`.

### Tournament (Loser) Tree
We use loser tree to merge the input buffers. It is implemented in `LoserTree` class inside `Losertree.h`. It provides `constructTree()` and `getNext()` functions. The loser tree is provided runs through a class called `RunStreamer` available in `RunStreamer.cpp`. This class can stream run from memory, from SSD and from HDD. Through this class, the loser tree remains oblivious, how the underlying next data is comming. The `RunStreamer` smartly pulls up the next chunk of runs from lower memory devices and fills the input buffers.
This facilitates performing a merge where some runs are originally in SSD and some are in HDD.

### Minimum Count of Rows
We use loser tree instead of winner tree or priority queue to minimize the number of comparisons during merging. The code for loser tree is available in `Losertree.h` inside `include` folder.

### Duplicate Removal
We remove duplicate during merging using Loser tree. When we remove a element from Loser tree, we first check if it is duplicate or not. If it is, then we skip this. It happens in three functions: `genMiniRuns()`,
`mergeSSDRuns()` and `mergeHDDRuns()` in `StorageTypes.cpp`. The code portions are in `StorageTypes.cpp:230-240`, `StorageTypes.cpp:520-530` and `StorageTypes.cpp:Line870-880`.

### Device-optimized Page Sizes
We use device-optimized page sizes which we configure by multiplying bandwidth and latency. We do this when setting up our devices in `configure()` function in `Storage.cpp:Line115-140`. 

### Spilling Memory to SSD
We spill our merged runs to SSD when our in-memory output buffer gets full. This code is available inside
the functions `genMiniRuns()`, `mergeSSDRuns()` and `mergeHDDRuns()` in  `StorageTypes.cpp` in `Line:250-300`, `Line:540-590` and `Line890-930`. We use the function `writeNextChunk()` to write to SSD which is inside `Storage.cpp:Line185-195`. 

### Spilling SSD to Disk
When merging runs stored in SSD using `mergeSSDRuns` and runs stored in HDD in `mergeHDDRuns`, we spill our merged runs to SSD first when our in-memory output buffer gets full. Eventually, the SSD output buffer gets full, we spill the merged runs to HDD then. 
We keep writing the run to SSD using `writeNextChunk()`. When the SSD gets full, this function instead of writing to SSD, starts a spill session by calling `spill()`. A spill session spills one particular merged run. Each device has a `spillTo` device set, which they use to spill the data. The `spill()` is inside `Storage.cpp:Line155-185`. The code for `startSpillSession()` and `endSpillSession()` is in `Storage.cpp:Line215-250`.

### Graceful Degradation
We perform graceful degradation, by first spilling Run to SSD and freeing up space in in-memory output buffer, and later when SSD output buffer gets filled, we free up space in SSD output buffer by spilling to HDD. The graceful degradation happens in `genMiniRuns()`, `mergeSSDRuns()` and `mergeHDDRuns()`. For example, in `genMiniRuns()`, we move some runs to SSD to make space for output buffer. The code is available in `StorageTypes.cpp:Line820-840`.

### Optimized Merge Patterns
We optimize merge pattern by minimizing access to disk, whenever possible. We utilize the SSD to store runs and only write to HDD, when the SSD gets full. For example, we merge in-memory miniRuns and store them in SSD. When the SSD gets full, we merge all the runs stored in SSD, and transfer the merged run to HDD. 
By writing long runs sequentially to disk, we make the best use of Disk when necessary. By allowing the merge of SSD and HDD runs together inside `mergeHDDRuns` function, we avoid multiple steps of merging SSD runs first and then HDD runs and make the most use of SSD capacity. 
The `externalMergeSort()` and `firstPass()` functions inside `Sort.cpp` can provide a high level overview of our strategy. 

### Verify Sort Order
We verify both the sort order and sort integrity. The order ensures the output is in sorted order in `verifyOrder()`. The integrity ensures all the input record are available in the output record except the duplicates in `verifyIntegrity()`. Both code are available in `Verify.cpp` in `Line60-130` and `Line 305-370`. The integrity check is done using hash partition. This can take a long time and requires large space. We smartly make it faster and memory efficient by hashing the records, which reduces both the partition size and the cost of comparisons.


## Example Results

**You can find example traces in this** [link](https://uwprod-my.sharepoint.com/:f:/g/personal/fislam2_wisc_edu/EhzBXejcjONImt8O0yjK4swBlzK4NjYLqI-4c9495Z3vAg?e=bfmE3A).

The following run durations are the result of running in cloudlab with nvme SSD. We show the result of runs in various settings.

### 1GB
`-c 1200000 -s 1000`
- Input_Gen Duration 4 seconds / 0 minutes
- External_Merge_Sort Total Duration 2 seconds / 0 minutes

### 10GB
`-c 6000000 -s 2048`
- Input_Gen Duration 47 seconds / 0 minutes
- External_Merge_Sort Total Duration 25 seconds / 0 minutes

### 25GB
`-c 1330000000 -s 20`
- Input_Gen Duration 111 seconds / 1 minutes
- External_Merge_Sort Total Duration 1300 seconds / 21 minutes

### 50GB
`-c 53000000 -s 1024 `
- Input_Gen Duration 206 seconds / 3 minutes
- External_Merge_Sort Total Duration 222 seconds / 3 minutes

### 125GB
`-c 132000000 -s 1024`
- Input_Gen Duration 519 seconds / 8 minutes
- External_Merge_Sort Total Duration 560 seconds / 9 minutes

## Members and Contributions

* Md. Tareq Mahmood
    * Initial setup
    * Quicksort
    * Cached Size Mini Runs
    * SSD DRAM sorting
    * Verify Sort Order
    * Test and results
    * Report
* Fariha Tabassum Islam
    * Tournament (Loser) Tree
    * Minimum Count of Rows
    * Device-optimized Page Sizes
    * Spilling (memory to SSD to HDD)
    * Graceful Degradation
    * Optimized Merge Patterns
