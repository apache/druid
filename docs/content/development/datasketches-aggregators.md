---
layout: doc_page
---

## DataSketches aggregator
Druid aggregators based on [datasketches]()http://datasketches.github.io/) library. You would ingest one or more metric columns using either `sketchBuild` or `sketchMerge` aggregators. Then, at query time, you can use `sketchMerge` with appropriate post aggregators described below. Note that sketch algorithms are approxiate, see details in the [datasketches doc](http://datasketches.github.io/docs/theChallenge.html).

#### aggregators to use at ingestion time

##### non-sketch input data
You can ingest data to druid and build the sketch objects which are later used at the time of querying to compute uniques.

```json
{ "type" : "sketchBuild", "name" : <output_name>, "fieldName" : <metric_name> }
```

##### sketch input data
You can ingest data to druid which already contains the sketch objects from batch pipeline by using [sketches-pig](https://github.com/DataSketches/sketches-pig) for example, in which case you just want to merge those and ingest into Druid.

```json
{ "type" : "sketchMerge", "name" : <output_name>, "fieldName" : <metric_name> }
```
#### aggregator to use at query time

```json
{ "type" : "sketchMerge", "name" : <output_name>, "fieldName" : <metric_name> }
```

Note that you can specify an additional field called "size" in above aggregators which is 16384 by default and must be a power of 2. At query time, size value must be greater than or equal to the value used at indexing time. Internally, size refers to the maximum number of entries sketch object will retain, higher size would mean higher accuracy but higher space needed to store those sketches. See [theta-size](http://datasketches.github.io/docs/ThetaSize.html) for more details. In general, I would recommend just sticking to default size which has worked well for us.

### Post Aggregators

#### Sketch Estimator
```json
{ "type"  : "sketchEstimate", "name": <output name>, "fieldName"  : <the name field value of the sketchMerge aggregator>}
```

#### Sketch Operations
```json
{ "type"  : "sketchSetOp", "name": <output name>, "func": <UNION|INTERSECT|NOT>, "fields"  : <the name field value of the sketchMerge aggregators>}
```

