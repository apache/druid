# Druid Watermark Collector and Keeper

The `org.apache.druid.extensions.watermarking` package provides 
2 additional druid server types: the `watermark-collector` and the
`watermark-keeper`. The primary function is to track data 
_completeness_, to grant application and infrastructure layers above 
Druid the ability to make decisions about when queries can issued, 
grounded in data rather than best effort guesses. It also provides 
auxiliary function as a live gap detector for the datasource timelines 
currently being served by the cluster.

## Timeline Metadata Collection

In order to achieve this, this extension introduces the more generic notion of 
_timeline metadata_ which can be described as metadata events
associated to a Druid datasource at a point in time or over an interval, 
as well as facilities to collect, compute, or fetch this data from the 
datasource segment timeline.

The `TimelineMetadata` interface defines data which may be collected 
from a timeline of segments, and `TimelineMetadataCursor` interface defines the mechanisms
to operate segment by segment, in chronological order across a timeline, to compute 
`TimelineMetadata`. 

`TimelineMetadataCollectorServerView` extends the `BrokerServerView` in order 
to provide a mechanism to enumerate the datasources available in the underlying 
inventory view.

The `TimelineMetadataCollector` is a generic class which can compute a result set of 
`TimelineMetadata` for a given interval of a datasource, given a set of 
`TimelineMetadataCursorFactories` and a `TimelineServerView`. 

## Watermarks

On top of this, we define a handful of interesting internal system 
_low_ and _high_ _watermarks_ as a type of this timeline metadata, that 
compute a `DateTime` by examining if segments match a certain criteria. Low 
watermarks are indicative of a contiguous timeline of satisfying segments, 
while high watermarks denote the most recent end boundary of satisfying segments. 
Additionally, as a convenience to consumers we collect two special data points, 
the maximum and minimum timestamps, to mark the boundaries for all segments 
of a datasource in the cluster.

* **maxtime** - the maximum segment end timestamp
* **mintime** - the minimum segment start timestamp
* **stable_low** - the low watermark of data completeness, which is the 
most recent timestamp of the largest contiguous timeline of a 
datasource. It is one of the most important data 
points of the system and designed to provide a conservative 
choice to operate within the interval of in order to ensure 
any query will be on complete data.
* **batch_low** - the batch completeness low watermark, which is the 
high point of the largest contiguous timeline of of data source 
that has been processed by batch indexing. In the best case for 
a real-time client batch is equivalent to complete, and this is 
always the case for batch only clients.
* **stable_high** - the high watermark for stable real-time data, which 
is defined as the segment hand-off point between real-time 
indexing tasks and historical nodes. Nominally for a real-time 
client complete is equivalent to stable. Note that this does 
not specify completeness, but potentially useful to applications 
which want to operate on stable real-time data without the 
guesswork.
* **batch_high** - the high watermark for batch indexing, does not
indicate completeness

## Watermark Collector

The `watermark-collector` operates internally in a manner similar to 
a portion of a normal Druid `broker`, but instead of using the segment 
information it observes in zookeeper to enable processing queries, it 
instead collects "significant" system watermarks and storing this 
_timeline metadata_. 
Specifically, this is achieved with the assistance of a couple of
 new types `WatermarkCollectorServerView` and `WatermarkCursor`. 
Essentially, the `WatermarkCollector` type sets handlers to _initialization_, 
_addition_ and _deletion_ of segments, and upon changes, processes a 
set of cursors across the relevant portion of the timeline. Persistent 
storage to write and read watermarks is exposed via the `WatermarkSink` 
and `WatermarkSource` modules respectively. Read and write operations 
are split in order to allow additional extensibility in the form of external 
processing pipelines, i.e. where the sink is a message queue and some 
outside task processes this data and exposes it as the source.


### WatermarkCursor

Upon appropriate events from the `WatermarkCollectorServerView`, 
the `WatermarkCollector` creates a list of type `WatermarkCursor` 
and performs a fold pattern to execute these cursors across the 
timeline of loaded segments, dropping individual cursors from 
the accumulator as  complete, and explicitly completing any 
remaining cursors when the end of time is reached. 
Upon _completion_, a cursor writes it's value to the watermark sink.

#### DataCompletenessLowWatermark

The low watermark of data completeness, which is the most recent 
timestamp of the largest contiguous timeline of a data source. 
This is lower bounded by the BatchCompletenessLowWatermark.

#### BatchCompletenessLowWatermark

The batch completeness low watermark, which is the high point of the 
largest contiguous timeline of of data source that has been processed 
by batch indexing.
 
#### StableDataHighWatermark

The high watermark for stable data, which is defined as the highest 
segment boundary that is effectively immutable, i.e. not being actively 
indexed in real-time.
 
#### BatchDataHighWatermark

The high watermark for batch data.

#### MaxtimeWatermark

This cursor signifies maximum segment end time.

#### MintimeWatermark

This cursor signifies minimum segment start time.

### WatermarkCursorFactory

`WatermarkCursor` implementations are created for each traversal 
of the segment timeline the `WatermarkCollector` performs with the 
help of implementations of `WatermarkCursorFactory`. Given a 
Druid datasource, interval, and set of segments, a `WatermarkCursorFactory` 
can construct the appropriate corresponding type of `WatermarkCursor`. 
By default, all `WatermarkCursor` implementations will be used, 
but this list can be overrriden by setting the 
`druid.watermarking.collector.cursors` property to the list of type
`WatermarkCursorFactory` classes to load.
 
## Watermark Keeper

The `watermark-keeper` exposes 
an API to retrieve, for a given Druid datasource, watermarks 
and watermark histories. This data does not require the live cluster state,
allowing them to be offloaded from the `watermark-collector` and easy horizontal
scaling of read-only capacity.


## Gap Detectors

Beyond the watermark system, a set of _gap detectors_ has also been implemented using the
`TimelineMetadata` infrastructure. Two implementations, the `SegmentGapDetector` and the 
`BatchGapDetector`, which detect any gaps, and specifically batch indexing gaps, are 
provided in the form of an API extension to the watermark collector.


## API

### Watermark Keeper API

##### `GET /druid/watermarking/v1/keeper/datasources/{dataSourceName}`

Retrieves the latest value for each kind of watermark for the 
specified Druid datasource.

_example response:_
```json
{
  "batch_low":"2017-07-24T12:00:00.000Z",
  "mintime":"2017-06-18T21:00:00.000Z",
  "batch_high":"2017-09-20T02:00:00.000Z",
  "stable_high":"2017-11-14T23:00:00.000Z",
  "stable_low":"2017-07-31T00:00:00.000Z",
  "maxtime":"2017-11-15T00:24:00.000Z"
}
```

##### `GET /druid/watermarking/v1/keeper/datasources/{dataSourceName}/{watermarkType}`
Retrieves the latest value for a specific kind of watermark 
for a Druid datasource.

_example response:_
```json
{"stable_high":"2017-11-15T00:00:00.000Z"}
```

##### `GET /druid/watermarking/v1/keeper/datasources/{dataSourceName}/{watermarkType}/history`
Retrieve a historic list of of watermark timestamps 
for a specific kind. Also includes collection time.

_example response:_

```
[
  {"timestamp":"2017-06-18T21:00:00.000Z","inserted":"2017-10-18T21:02:23.383Z"},
  {"timestamp":"2017-06-06T10:00:00.000Z","inserted":"2017-10-06T09:01:08.790Z"},
  ...
  {"timestamp":"2016-08-10T17:00:00.000Z","inserted":"2017-08-10T16:54:04.980Z"},
  {"timestamp":"2016-05-12T00:00:00.000Z","inserted":"2017-08-03T12:12:23.439Z"}
]
```

### Watermark Collector API

In addition to hosting the API exposed by the `watermark-keeper` node, the 
`watermark-collector` provides means to override _low_ watermark values to deal
with cases of data gaps which should legitimately be ignored when considering
timeline completeness, as well as some additional management functions. Not all 
watermark types can be overridden, this functionality is watermark type specific.
 
##### `POST /druid/watermarking/v1/collector/datasources/{dataSourceName}/{watermarkType}?timestamp={timestamp}`

Override a watermark to move it's value forward.

_example:_

```
POST /druid/watermarking/v1/collector/datasources/metrics_druid/stable_low?timestamp=2017-07-24T12:00:00.000Z`
```

##### `POST /druid/watermarking/v1/collector/datasources/{dataSourceName}/{watermarkType}/rollback?timestamp={timestamp}`

Override a watermark to move it's value into the past.

_example:_

```
POST /druid/watermarking/v1/collector/datasources/metrics_druid/stable_low/rollback?timestamp=2017-07-24T12:00:00.000Z`
```

##### `POST /druid/watermarking/v1/collector/datasources/{dataSourceName}/{watermarkType}/purge?timestamp={timestamp}`

Remove all watermark history older than timestamp, by _collection_, not actual watermark value, if `WatermarkSink` supports this operation.

_example:_

```
POST /druid/watermarking/v1/collector/datasources/mmx_metrics_druid/stable_low/purge?timestamp=2017-05-01T00:00:00.000Z`
```

##### `GET /druid/watermarking/v1/collector/loadstatus`

`TimelineMetadataCollectorServerview` initialization state 

_example response:_

```
{
  "inventoryInitialized": true,
  "watermarks": {
    "metrics_kafka": {
      "batch_low": "2017-07-24T12:00:00.000Z",
      ...
    },
    "metrics_druid": {
      "batch_low": "2017-07-24T12:00:00.000Z",
      ...
    },
    ...
  }
}
```

##### `GET /druid/watermarking/v1/collector/watermarks`

Watermark values for all datasources 

_example response:_

```
{
  "inventoryInitialized": true,
  "watermarks": {
    "metrics_kafka": {
      "batch_low": "2017-07-24T12:00:00.000Z",
      ...
    },
    "metrics_druid": {
      "batch_low": "2017-07-24T12:00:00.000Z",
      ...
    },
    ...
  }
}
```

### Gap Detector API

##### `GET /druid/watermarking/v1/collector/datasources/{dataSourceName}/gaps`

Retrieves the list of intervals defining gaps for each type of gap detector hosted by the 
server.

_example response:_

```
{
  "data": [
    "2016-11-15T00:00:00.000Z/2016-11-15T01:00:00.000Z",
    "2016-12-28T10:00:00.000Z/2016-12-28T15:00:00.000Z",
    ...
    "2017-07-31T00:00:00.000Z/2017-07-31T03:00:00.000Z",
    "2017-11-04T05:00:00.000Z/2017-11-04T18:00:00.000Z"
  ],
  "batch": [
    "2016-11-15T00:00:00.000Z/2016-11-15T01:00:00.000Z",
    "2016-12-18T21:00:00.000Z/2016-12-29T20:00:00.000Z",
    ...
    "2017-09-08T23:00:00.000Z/2017-09-14T19:00:00.000Z",
    "2017-09-14T20:00:00.000Z/2017-09-16T00:00:00.000Z"
  ]
}
```

##### `GET /druid/watermarking/v1/collector/datasources/{dataSourceName}/gaps/{gapType}`

Retrieves the list of intervals defining gaps for the requested gap type.

_example response:_

```json
{"data":["2016-12-28T10:00:00.000Z/2016-12-28T15:00:00.000Z","2017-03-16T03:00:00.000Z/2017-03-16T04:00:00.000Z","2017-03-25T08:00:00.000Z/2017-03-25T18:00:00.000Z","2017-03-31T18:00:00.000Z/2017-03-31T19:00:00.000Z","2017-07-31T00:00:00.000Z/2017-07-31T03:00:00.000Z","2017-11-04T05:00:00.000Z/2017-11-04T18:00:00.000Z"]}
```


### Status API

The status api exposes methods to get all watermarks and gap detected for datasources

##### `GET /druid/watermarking/v1/collector/status`

Watermark values for all datasources and all current gaps in the timeline,
which will be explained in greater detail

_example response:_

```
{
  "inventoryInitialized": true,
  "datasources": {
    "metrics_druid": {
      "watermarks": {
        "batch_low": "2017-04-06T00:00:00.000Z",
        "mintime": "2016-07-01T00:00:00.000Z",
        "batch_high": "2017-04-09T00:00:00.000Z",
        "stable_low": "2017-04-06T00:00:00.000Z",
        "stable_high": "2017-04-09T00:00:00.000Z",
        "maxtime": "2017-04-09T00:00:00.000Z"
      },
      "gaps": {
        "data": [
          "2017-04-06T00:00:00.000Z/2017-04-07T00:00:00.000Z"
        ],
        "batch": [
          "2017-04-06T00:00:00.000Z/2017-04-07T00:00:00.000Z"
        ]
      }
    }
  }
}
```

##### `GET /druid/watermarking/v1/collector/status`

Watermark values for all datasources and all current gaps in the timeline,
which will be explained in greater detail

_example response:_

```
{
  "inventoryInitialized": true,
  "metrics_druid": {
    "watermarks": {
      "batch_low": "2017-04-06T00:00:00.000Z",
      "mintime": "2016-07-01T00:00:00.000Z",
      "batch_high": "2017-04-09T00:00:00.000Z",
      "stable_low": "2017-04-06T00:00:00.000Z",
      "stable_high": "2017-04-09T00:00:00.000Z",
      "maxtime": "2017-04-09T00:00:00.000Z"
    },
    "gaps": {
      "data": [
        "2017-04-06T00:00:00.000Z/2017-04-07T00:00:00.000Z"
      ],
      "batch": [
        "2017-04-06T00:00:00.000Z/2017-04-07T00:00:00.000Z"
      ]
    }
  }
}
```

## Config

### WatermarkCollector

In addition to the druid broker configuration options, thh `watermark-collector`
also accepts:

- `traceParallelism` sets number of stripes in lock for tracing timelines
- `traceAnchorWatermark` sets default starting point of timeline trace on 
segment add and remove events, defaulting to 'batch'. Must specify a valid 
`WatermarkCursor` 'type'.
- `cursors` extensibility point, allows setting a custom list of watermark 
cursors by supplying a list of `WatermarkCursorFactory` implementations.
- `gapDetectors` extensibility point, allows setting custom list of gap 
detectors by supplying a list of `GapDetectorFactory` implementations.
 
```$INI
druid.watermarking.collector.traceParallelism=16
druid.watermarking.collector.traceAnchorWatermark=complete
druid.watermarking.collector.cursors=["org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermarkFactory", "org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermarkFactory"]
druid.watermarking.collector.gapDetectors=["org.apache.druid.extensions.watermarking.gaps.BatchGapDetectorFactory", "org.apache.druid.extensions.watermarking.gaps.SegmentGapDetectorFactory"]
```

By default, all
factories for the cursors provided by `org.apache.druid.extensions.watermarking.watermarks` are used

```
BatchCompletenessLowWatermarkFactory
DataCompletenessLowWatermarkFactory
MaxtimeWatermarkFactory
MintimeWatermarkFactory
StableDataHighWatermarkFactory
```

as well as all gap detectors defined in `org.apache.druid.extensions.watermarking.gaps`

```
BatchGapDetectorFactory
SegmentGapDetectorFactory
```

### Watermark Sources, Sinks, and Stores Config

The `watermark-collector` requires both a `sink` and `source` property 
which refer by key to an entry in the `store` property. Internally 
these map to the `WatermarkSink` and `WatermarkSource` 
interfaces, with a combined interface `WatermarkStore`, and 
are provided by `WatermarkSinkModule`, `WatermarkSourceModule` 
and `WatermarkStore` modules. There are currently 3 implementations,
which provide a `WatermarkStore`, and so can be used as 
either `WatermarkSink` or `WatermarkSource`.

#### `WatermarkCache`

By default, the watermark cursors read data through an in
memory cache with a 1 minute expiration, to ease read rates
on the configured `WatermarkSource` which can become
 quite high depending on the rate of segment change.
```$INI
druid.watermarking.cache.enable=true
druid.watermarking.cache.expireMinutes=1
druid.watermarking.cache.maxSize=10000
```

#### `MemoryWatermarkStore`

The `MemoryWatermarkStore` provides an in memory implementation 
of the `WatermarkStore` interface. Note that the `MemoryWatermarkStore` 
MUST be used synchronously, i.e. as both source and sink, as it has 
no mechanism to communicate out of process.

```$INI
druid.watermarking.sink.type=memory
druid.watermarking.source.type=memory
```

##### Store config

```$INI
druid.watermarking.store.memory.maxHistoricalEntries=1000
```

#### `MySqlWatermarkStore`

Provides a MySQL backed implementation of `WatermarkStore`.

```$INI
druid.watermarking.sink.type=mysql
druid.watermarking.source.type=mysql
```

##### Store config

```$INI
druid.watermarking.store.mysql.host=localhost
druid.watermarking.store.mysql.port=32768
druid.watermarking.store.mysql.user=druid
druid.watermarking.store.mysql.password=diurd
druid.watermarking.store.mysql.connectURI=jdbc:mysql://localhost:32768/local?characterEncoding=UTF-8
druid.watermarking.store.mysql.createTimelineTables=true
```

#### `DatastoreWatermarkStore`

Provides as a Google Cloud Datastore backed implementation of 
`WatermarkStore`.

```$INI
druid.watermarking.sink.type=datastore
druid.watermarking.source.type=datastore
```

##### Store config

```$INI
druid.watermarking.store.datastore.projectId=someproject
druid.watermarking.store.datastore.accessKeyPath=/path/to/key.json
```

#### `PubsubWatermarkSink`

Provides Google cloud pubsub `WatermarkSink`.
```$INI
druid.watermarking.sink.type=pubsub
```

##### Store config

```$INI
druid.watermarking.store.pubsub.projectId=someproject
druid.watermarking.store.pubsub.topic=watermarking-test
```

#### `CompositeWatermarkSink`

Provides `WatermarkSink` demux to a list of sinks. For example, 
imagine a scenario where we wish to run a `WatermarkCollector` 
that uses only in memory storage for it's decision logic, but 
writes to pubsub where some sort of external processing makes it 
available to the store of a `WatermarkKeeper` which 
serves actual client queries for watermark data.

```$INI
druid.watermarking.sink.type=composite
druid.watermarking.source.type=memory
```

##### Store config
```$INI
druid.watermarking.store.composite.sinks=["org.apache.druid.extensions.watermarking.storage.memory.MemoryWatermarkStore","org.apache.druid.extensions.watermarking.storage.google.PubsubWatermarkSink"]
```

#### Asynchronous Configuration

i.e.

```$INI
druid.watermarking.sink.type=pubsub
druid.watermarking.source.type=datastore
```

```$INI
druid.watermarking.sink.type=composite
druid.watermarking.source.type=memory
```
