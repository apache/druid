---
layout: doc_page
---
# Tasks Overview

Tasks are run on middle managers and always operate on a single data source.

Tasks are submitted using POST requests to the Overlord. Please see [Overlord Task API](../operations/api-reference.html#overlord-tasks) for API details.

There are several different types of tasks.

## Segment Creation Tasks

### Hadoop Index Task

See [batch ingestion](../ingestion/hadoop.html).

### Native Index Tasks

Druid provides a native index task which doesn't need any dependencies on other systems.
See [native index tasks](./native_tasks.html) for more details.

### Kafka Indexing Tasks

Kafka Indexing tasks are automatically created by a Kafka Supervisor and are responsible for pulling data from Kafka streams. These tasks are not meant to be created/submitted directly by users. See [Kafka Indexing Service](../development/extensions-core/kafka-ingestion.html) for more details.

### Stream Push Tasks (Tranquility)

Tranquility Server automatically creates "realtime" tasks that receive events over HTTP using an [EventReceiverFirehose](../ingestion/firehose.html#eventreceiverfirehose). These tasks are not meant to be created/submitted directly by users. See [Tranquility Stream Push](../ingestion/stream-push.html) for more info.

## Compaction Tasks

Compaction tasks merge all segments of the given interval. Please see [Compaction](../ingestion/compaction.html) for details.

## Segment Merging Tasks

<div class="note info">
The documentation for the Append Task, Merge Task, and Same Interval Merge Task has been moved to <a href="../ingestion/misc-tasks.html">Miscellaneous Tasks</a>.
</div>

## Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. 

Please see [Deleting Data](../ingestion/delete-data.html) for details.

## Misc. Tasks

Please see [Miscellaneous Tasks](../ingestion/misc-tasks.html).

## Task Locking and Priority

Please see [Task Locking and Priority](../ingestion/locking-and-priority.html).