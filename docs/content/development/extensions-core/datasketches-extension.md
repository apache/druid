---
layout: doc_page
---

## DataSketches extension

Druid aggregators based on [datasketches](http://datasketches.github.io/) library. Sketches are data structures implementing approximate streaming mergeable algorithms. Sketches can be ingested from the outside of Druid or built from raw data at ingestion time. Sketches can be stored in Druid segments as additive metrics.

To use the datasketches aggregators, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

The following modules are available:

* [Theta sketch](datasketches-theta.html) - approximate distinct counting with set operations (union, intersection and set difference).
* [Tuple sketch](datasketches-tuple.html) - extension of Theta sketch to support values associated with distinct keys (arrays of numeric values in this specialized implementation)
* [Quantiles sketch](datasketches-quantiles.html) - approximate distribution of comparable values to obtain ranks, quantiles and histograms. This is a specialized implementation for numeric values.
