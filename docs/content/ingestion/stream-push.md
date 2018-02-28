---
layout: doc_page
---

## Stream Push

Druid can connect to any streaming data source through
[Tranquility](https://github.com/druid-io/tranquility/blob/master/README.md), a package for pushing
streams to Druid in real-time. Druid does not come bundled with Tranquility, and you will have to download the distribution.

<div class="note info">
If you've never loaded streaming data into Druid, we recommend trying out the
<a href="../tutorials/tutorial-streams.html">stream loading tutorial</a> first and then coming back to this page.
</div>

Note that with all streaming ingestion options, you must ensure that incoming data is recent
enough (within a [configurable windowPeriod](#segmentgranularity-and-windowperiod) of the current
time). Older messages will not be processed in real-time. Historical data is best processed with
[batch ingestion](../ingestion/batch-ingestion.html).

### Server

Druid can use [Tranquility Server](https://github.com/druid-io/tranquility/blob/master/docs/server.md), which
lets you send data to Druid without developing a JVM app. You can run Tranquility server colocated with Druid middleManagers
and historical processes.

Tranquility server is started by issuing:

```bash
bin/tranquility server -configFile <path_to_config_file>/server.json
```

To customize Tranquility Server:

- In `server.json`, customize the `properties` and `dataSources`.
- If you have servers already running Tranquility, stop them (CTRL-C) and start
them up again.

For tips on customizing `server.json`, see the
*[Loading your own streams](../tutorials/tutorial-streams.html)* tutorial and the
[Tranquility Server documentation](https://github.com/druid-io/tranquility/blob/master/docs/server.md).

### Kafka

[Tranquility Kafka](https://github.com/druid-io/tranquility/blob/master/docs/kafka.md)
lets you load data from Kafka into Druid without writing any code. You only need a configuration
file.

Tranquility server is started by issuing:

```bash
bin/tranquility kafka -configFile <path_to_config_file>/kafka.json
```

To customize Tranquility Kafka in the single-machine quickstart configuration:

- In `kafka.json`, customize the `properties` and `dataSources`.
- If you have Tranquility already running, stop it (CTRL-C) and start it up again.

For tips on customizing `kafka.json`, see the
[Tranquility Kafka documentation](https://github.com/druid-io/tranquility/blob/master/docs/kafka.md).

### JVM apps and stream processors

Tranquility can also be embedded in JVM-based applications as a library. You can do this directly
in your own program using the
[Core API](https://github.com/druid-io/tranquility/blob/master/docs/core.md), or you can use
the connectors bundled in Tranquility for popular JVM-based stream processors such as
[Storm](https://github.com/druid-io/tranquility/blob/master/docs/storm.md),
[Samza](https://github.com/druid-io/tranquility/blob/master/docs/samza.md),
[Spark Streaming](https://github.com/druid-io/tranquility/blob/master/docs/spark.md), and
[Flink](https://github.com/druid-io/tranquility/blob/master/docs/flink.md).

## Concepts

### Task creation

Tranquility automates creation of Druid realtime indexing tasks, handling partitioning, replication,
service discovery, and schema rollover for you, seamlessly and without downtime. You never have to
write code to deal with individual tasks directly. But, it can be helpful to understand how
Tranquility creates tasks.

Tranquility spawns relatively short-lived tasks periodically, and each one handles a small number of
[Druid segments](../design/segments.html). Tranquility coordinates all task
creation through ZooKeeper. You can start up as many Tranquility instances as you like with the same
configuration, even on different machines, and they will send to the same set of tasks.

See the [Tranquility overview](https://github.com/druid-io/tranquility/blob/master/docs/overview.md)
for more details about how Tranquility manages tasks.

### segmentGranularity and windowPeriod

The segmentGranularity is the time period covered by the segments produced by each task. For
example, a segmentGranularity of "hour" will spawn tasks that create segments covering one hour
each.

The windowPeriod is the slack time permitted for events. For example, a windowPeriod of ten minutes
(the default) means that any events with a timestamp older than ten minutes in the past, or more
than ten minutes in the future, will be dropped.

These are important configurations because they influence how long tasks will be alive for, and how
long data stays in the realtime system before being handed off to the historical nodes. For example,
if your configuration has segmentGranularity "hour" and windowPeriod ten minutes, tasks will stay
around listening for events for an hour and ten minutes. For this reason, to prevent excessive
buildup of tasks, it is recommended that your windowPeriod be less than your segmentGranularity.

### Append only

Druid streaming ingestion is *append-only*, meaning you cannot use streaming ingestion to update or
delete individual records after they are inserted. If you need to update or delete individual
records, you need to use a batch reindexing process. See the *[batch ingest](batch-ingestion.html)*
page for more details.

Druid does support efficient deletion of entire time ranges without resorting to batch reindexing.
This can be done automatically through setting up retention policies.

### Guarantees

Tranquility operates under a best-effort design. It tries reasonably hard to preserve your data, by allowing you to set
up replicas and by retrying failed pushes for a period of time, but it does not guarantee that your events will be
processed exactly once. In some conditions, it can drop or duplicate events:

- Events with timestamps outside your configured windowPeriod will be dropped.
- If you suffer more Druid Middle Manager failures than your configured replicas count, some
partially indexed data may be lost.
- If there is a persistent issue that prevents communication with the Druid indexing service, and
retry policies are exhausted during that period, or the period lasts longer than your windowPeriod,
some events will be dropped.
- If there is an issue that prevents Tranquility from receiving an acknowledgement from the indexing
service, it will retry the batch, which can lead to duplicated events.
- If you are using Tranquility inside Storm or Samza, various parts of both architectures have an
at-least-once design and can lead to duplicated events.

Under normal operation, these risks are minimal. But if you need absolute 100% fidelity for
historical data, we recommend a [hybrid batch/streaming](../tutorials/ingestion.html#hybrid-batch-streaming)
architecture.

### Deployment Notes

Stream ingestion may generate a large number of small segments because it's difficult to optimize the segment size at
ingestion time. The number of segments will increase over time, and this might cuase the query performance issue. 

Details on how to optimize the segment size can be found on [Segment size optimization](../../operations/segment-optimization.html).

## Documentation

Tranquility documentation be found [here](https://github.com/druid-io/tranquility/blob/master/README.md).

## Configuration

Tranquility configuration can be found [here](https://github.com/druid-io/tranquility/blob/master/docs/configuration.md).

Tranquility's tuningConfig can be found [here](http://static.druid.io/tranquility/api/latest/#com.metamx.tranquility.druid.DruidTuning). 
