---
layout: doc_page
---

### Benchmarks
The following benchmarks exist in the io.druid.benchmark package and may be useful as a starting point for developers 
interested in evaluating the performance impact of potential changes.

#### ColumnScanBenchmark
* Compares the difference between filtering on a column via a full scan and filtering on a column using its dictionary and bitmap indexes. 
* The benchmark functions apply an extraction filter to the __time column and a String dimension containing ISO8601 timestamps.

#### CompressedIndexedIntsBenchmark
Description TBD

#### CompressedVSizeIndexedBenchmark
Description TBD

#### ConciseComplementBenchmark
Description TBD

#### FlattenJSONBenchmark
* Evaluates performance of processing nested JSON events using the JSONPathParser from druid-api.
* FlattenJSONBenchmarkUtil in the benchmarks package provides functions for generating nested JSON events.

#### IncrementalIndexAddRowsBenchmark
* Evaluates the performance of ingesting events in IncrementalIndex with different dimension value types.
* There are benchmark functions for testing ingestion of String, Long, and Float dimensions.

#### IndexedIntsWrappingBenchmark
* Evaluates performance of primitive boxing/unboxing, type casting, and use of a wrapper class when reading from a list of IndexedInts.

#### MergeSequenceBenchmark
Description TBD

#### QueryableIndexLoadingBenchmark
* Compares the performance difference between reading from a QueryableIndex using IndexedInts directly and via a wrapper class that casts integer primitives to Comparable

#### StupidPoolConcurrencyBenchmark
Description TBD

