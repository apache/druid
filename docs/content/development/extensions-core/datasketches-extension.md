---
layout: doc_page
---

## DataSketches extension

Druid aggregators based on [datasketches](http://datasketches.github.io/) library.  Sketches are data structures implementing approximate streaming mergeable algorithms. Sketches can be ingested from the outside of Druid or built from raw data at ingestion time. Sketches can be stored in Druid segments as additive metrics.

To use the datasketch aggregators, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

The following aggregators are available:

1. [Theta sketch](datasketches-theta.html), useful for approximate set counting, and supporting union, intersection, and difference operations.
2. [Quantiles sketch](datasketches-quantiles.html).
3. [Tuple sketch](datasketches-tuple.html).
