---
layout: doc_page
---
# Merge Existing Segments

Druid can automatically merge small segments into a segment that has a more optimal segment size. You can turn that on by specifying a merge
strategy at `druid.coordinator.merge.strategy` (See [Coordinator Configuration](../configuration/coordinator.html)). 
Currently there are two merge strategies you can choose for Druid.

#### `append`
This simply sends an [Append Task](../ingestion/tasks.html) with segments that need to be merged. To help Druid decide what segments need to be merged,
user can set appropriate dynamic configurations values for `mergeBytesLimit` and `mergeSegmentsLimit` (See [Coordinator Dynamic Configuration]("../configuration/coordinator.html#dynamic-configuration").). 
Note that Append task will only merge segments that have a single shard.



#### `hadoop`
This sends a Hadoop Reindex Task to actually reindex the intervals covered by segments that have imbalanced sizes. Behind the scene, Coordinator will periodically
find segments whose sizes are small. Once it finds enough such segments whose current total size is greater than the threshold specified by `mergeBytesLimit` 
(See [Coordinator Dynamic Configuration]("../configuration/coordinator.html#dynamic-configuration")), Coordinator will submit a Hadoop Reindex Task to reindex
those small segments.

To enable this, user will need to post
a `hadoopMergeConfig`, along with other dynamic configurations to [Coordinator Dynamic Configuration end point]("../configuration/coordinator.html#dynamic-configuration"). 
Optionally, user can also configure it in Coordinator console.

`hadoopMergeConfig` provides information about how Coordinator finds imbalanced segments, and how do to Hadoop reindexing.

Here is what goes inside `hadoopMergeConfig`,

|Field|Type|Description|Default|Required|
|-----|----|-----------|-------|--------|
|keepGap|Boolean.|Indicate whether Druid should merge segments whose intervals are non-contiguous. For example, segment A has interval `2016-03-22/2016-03-23`, segment B has interval `2016-03-24/2016-03-25`. If `keepGap` is true, Druid will not merge A and B.|false|no|
|hadoopDependencyCoordinates|Array of String.|A list of Hadoop dependency coordinates that Druid will use, this property will override the default Hadoop coordinates. Once specified, Druid will look for those Hadoop dependencies from the location specified by `druid.extensions.hadoopDependenciesDir`|null|no|
|tuningConfig|JSON Object.|This is exactly same as the tuningConfig specified in Hadoop Index Task. See [TuningConfig](../ingestion/batch-ingestion.html#tuningconfig).|null|no|
|hadoopMergeSpecs|Array of HadoopMergeSpec.|A list of HadoopMergeSpec. Each data source will have its own HadoopMergeSpec. See below.|null|no|

For each data source on which you want to enable automatic merge, you would need to provide a `HadoopMergeSpec` so that Druid knows how to reindex those segments.
Here is what goes inside `HadoopMergeSpec`

|Field|Type|Description|Default|Required|
|-----|----|-----------|-------|--------|
|dataSource|String.|The data source of segments that will be automatically merged.|Value has to be provided by user.|yes|
|queryGranularity|String.|Defines the granularity of the query while loading data. See [Granularities](../querying/granularities.html).|NONE|no|
|dimensions|Array of String.|Name of dimension columns to load.|The super set of dimensions in the existing segments.|no|
|metricsSpec|Array of aggregators.|A list of [aggregators](../querying/aggregations.html).|Value has to be provided by user.|yes|

Example:

```json
{
  ... other Coordinator dynamic configs...
  "mergeBytesLimit": 500000000,
  "hadoopMergeConfig": {
    "keepGap": true,
    "hadoopDependencyCoordinates": null,
    "tuningConfig": null,
    "hadoopMergeSpecs": [
      {
        "dataSource": "wikipedia",
        "queryGranularity": "DAY",
        "dimensions": ["language"],
        "metricsSpec": [
          {
            "type": "count",
            "name": "count"
          },
          {
            "type": "doubleSum",
            "name": "added",
            "fieldName": "added"
          },
          {
            "type": "doubleSum",
            "name": "deleted",
            "fieldName": "deleted"
          },
          {
            "type": "doubleSum",
            "name": "delta",
            "fieldName": "delta"
          }
        ]
      }
    ]
  }
}
```

With this configuration posted to [Coordinator Dynamic Configuration end point]("../configuration/coordinator.html#dynamic-configuration"), 
Coordinator will automatically merge imbalanced segments whose data sources are "wikipedia". Once it finds enough small segments whose total
size is greater or equal than `mergeBytesLimit`(in the example, it is configured to 500MB on-disk size), it will submit a Hadoop Index Task to reindex intervals
covered by those imbalanced segments, using the dimensions specified in `dimensions` (if not specified, it will use the `dimensions` in the existing segments) and aggregators specified in `metricsSpec`.
