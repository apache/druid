---
layout: doc_page
---
Real-time Node
==============
For Real-time Node Configuration, see [Realtime Configuration](Realtime-Config.html).

For Real-time Ingestion, see [Realtime Ingestion](Realtime-ingestion.html).

Realtime nodes provide a realtime index. Data indexed via these nodes is immediately available for querying. Realtime nodes will periodically build segments representing the data they’ve collected over some span of time and transfer these segments off to [Historical](Historical.html) nodes. They use ZooKeeper to monitor the transfer and MySQL to store metadata about the transfered segment. Once transfered, segments are forgotten by the Realtime nodes.

### Running

```
io.druid.cli.Main server realtime
```
Segment Propagation
-------------------

The segment propagation diagram for real-time data ingestion can be seen below:

![Segment Propagation](../img/segmentPropagation.png "Segment Propagation")

You can read about the various components shown in this diagram under the Architecture section (see the menu on the left).

### Firehose

See [Firehose](Firehose.html).

### Plumber

See [Plumber](Plumber.html)

Extending the code
------------------

Realtime integration is intended to be extended in two ways:

1.  Connect to data streams from varied systems ([Firehose](https://github.com/druid-io/druid-api/blob/master/src/main/java/io/druid/data/input/FirehoseFactory.java))
2.  Adjust the publishing strategy to match your needs ([Plumber](https://github.com/metamx/druid/blob/master/server/src/main/java/io/druid/segment/realtime/plumber/PlumberSchool.java))

The expectations are that the former will be very common and something that users of Druid will do on a fairly regular basis. Most users will probably never have to deal with the latter form of customization. Indeed, we hope that all potential use cases can be packaged up as part of Druid proper without requiring proprietary customization.

Given those expectations, adding a firehose is straightforward and completely encapsulated inside of the interface. Adding a plumber is more involved and requires understanding of how the system works to get right, it’s not impossible, but it’s not intended that individuals new to Druid will be able to do it immediately.
