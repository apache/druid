---
layout: doc_page
---

## DataSketches aggregator experiments

Experimental Druid aggregators based on [datasketches](http://datasketches.github.io/) library. Note that sketch algorithms are approximate. 

To use the datasketch aggregators, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches-experiments"]
```

## quantiles aggregator

<div class="note caution">
Experimental aggregator. API may change in backwards incompatible ways.
</div>

This aggregator is used to compute quantiles and histograms. Only double data type (other number types would be coerced) can be used as input to this aggregator.

At ingestion time, quantiles sketches would get created and stored in the segments. At query time, quantile sketches can be used to compute quantiles and histograms from that column. 

Compared to [approximate-histograms](approximate-histograms.html), this aggregator lets you tune the sketch size to control accuracy, see [quantiles-accuracy](http://datasketches.github.io/docs/QuantilesAccuracy.html) and may perform better with outliers in the data points.

### Aggregators

```json
{
  "type" : "datasketchesQuantilesSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,

  //following boolean field is optional. This should only be used at
  //indexing time if your input data contains quantile sketch objects.
  //that would be the case if you use datasketches library outside of Druid,
  //say with Pig/Hive, to produce the data that you are ingesting into Druid
  "isInputSketch": false

  //following field is optional, default = 1024.
  //it must be a power of 2 and less than 65536
  //default value here is arbitrary and appropriate value for specific usecase
  //should be determined.
  //higher size value would provide better accuracy.
  //you should use same or greater size at query time to get desired accuracy.
  "size": 1024
 }
```

### Post Aggregators

#### Quantile Estimator

```json
{
  "type"  : "datasketchesQuantile",
  "name": <output name>,
  "fraction" : <double quantile to compute>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```

#### Quantiles Estimator

```json
{
  "type"  : "datasketchesQuantiles",
  "name": <output name>,
  "fractions" : <array of double for quantiles to compute>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```

#### Custom Splits Histogram Estimator

```json
{
  "type"  : "datasketchesCustomSplitsHistogram",
  "name": <output name>,
  "splits" : <array of double for split points in input values>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```

#### Equal Splits Histogram Estimator

```json
{
  "type"  : "datasketchesEqualSplitsHistogram",
  "name": <output name>,
  "numSplits" : <number of bins, must be greater than 1>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```

#### Min Estimator
Returns minimum value stored in the sketch.

```json
{
  "type"  : "datasketchesQuantilesSketchMin",
  "name": <output name>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```

#### Max Estimator
Returns maximum value stored in the sketch.

```json
{
  "type"  : "datasketchesQuantilesSketchMax",
  "name": <output name>,
  "fieldName" : <the name field value of the quantile sketch aggregator>
}
```
