---
layout: doc_page
---

## Load your own batch data

Before you get started with loading your own batch data, you should have first completed the [quickstart](quickstart.html).

You can easily load any timestamped dataset into Druid. For Druid batch loads, the most important 
questions are:

  * What should the dataset be called? This is the "dataSource" field of the "dataSchema".
  * Where is the dataset located? The file paths belong in the "paths" of the "inputSpec". If you
want to load multiple files, you can provide them as a comma-separated string.
  * Which field should be treated as a timestamp? This belongs in the "column" of the "timestampSpec".
  * Which fields should be treated as dimensions? This belongs in the "dimensions" of the "dimensionsSpec".
  * Which fields should be treated as metrics? This belongs in the "metricsSpec".
  * What time ranges (intervals) are being loaded? This belongs in the "intervals" of the "granularitySpec".

```note-info
If your data does not have a natural sense of time, you can tag each row with the current time. 
You can also tag all rows with a fixed timestamp, like "2000-01-01T00:00:00.000Z".
```

Let's use this pageviews dataset as an example. Druid supports TSV, CSV, and JSON out of the box. 
Note that nested JSON objects are not supported, so if you do use JSON, you should provide a file 
containing flattened objects.

```json
{"time": "2015-09-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
{"time": "2015-09-01T01:00:00Z", "url": "/", "user": "bob", "latencyMs": 11}
{"time": "2015-09-01T01:30:00Z", "url": "/foo/bar", "user": "bob", "latencyMs": 45}
```

Make sure the file has no newline at the end. If you save this to a file called "pageviews.json", then for this dataset:

  * Let's call the dataset "pageviews".
  * The data is located in "pageviews.json".
  * The timestamp is the "time" field.
  * Good choices for dimensions are the string fields "url" and "user".
  * Good choices for metrics are a count of pageviews, and the sum of "latencyMs". Collecting that 
sum when we load the data will allow us to compute an average at query time as well.
  * The data covers the time range 2015-09-01 (inclusive) through 2015-09-02 (exclusive).

You can copy the existing `quickstart/wikiticker-index.json` indexing task to a new file:

```bash
cp quickstart/wikiticker-index.json quickstart/pageviews-index.json
```

And modify it by altering these sections:

```json
"dataSource": "pageviews"
```

```json
"inputSpec": {
  "type": "static",
  "paths": "pageviews.json"
}
```

```json
"timestampSpec": {
  "format": "auto",
  "column": "time"
}
```

```json
"dimensionsSpec": {
  "dimensions": ["url", "user"]
}
```

```json
"metricsSpec": [
  {"name": "views", "type": "count"},
  {"name": "latencyMs", "type": "doubleSum", "fieldName": "latencyMs"}
]
```

```json
"granularitySpec": {
  "type": "uniform",
  "segmentGranularity": "day",
  "queryGranularity": "none",
  "intervals": ["2015-09-01/2015-09-02"]
}
```

Finally, fire off the task and indexing will proceed!

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/pageviews-index.json localhost:8090/druid/indexer/v1/task
```

If anything goes wrong with this task (e.g. it finishes with status FAILED), you can troubleshoot  
by visiting the "Task log" on the [overlord console](http://localhost:8090/console.html).

```note-info
Druid supports a wide variety of data formats, ingestion options, and configurations not 
discussed here. For a full explanation of all available features, see the ingestion sections of the Druid 
documentation.
```

For more information on loading batch data, please see [the batch ingestion documentation](../ingestion/batch-ingestion.html).
