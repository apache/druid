---
layout: doc_page
---
Ingesting from Kafka 8
----------------------

The previous examples are for Kafka 7. To support Kafka 8, a couple changes need to be made:

- Update realtime node's configs for Kafka 8 extensions
  - e.g.
    - `druid.extensions.coordinates=[...,"io.druid.extensions:druid-kafka-seven:0.6.171",...]`
    - becomes
    - `druid.extensions.coordinates=[...,"io.druid.extensions:druid-kafka-eight:0.6.171",...]`
- Update realtime task config for changed keys
  - `firehose.type`, `plumber.rejectionPolicyFactory`, and all of `firehose.consumerProps` changes.

```json

    "firehose" : {
        "type" : "kafka-0.8",
        "consumerProps" : {
            "zookeeper.connect": "localhost:2181",
            "zookeeper.connection.timeout.ms": "15000",
            "zookeeper.session.timeout.ms": "15000",
            "zookeeper.sync.time.ms": "5000",
            "group.id": "topic-pixel-local",
            "fetch.message.max.bytes": "1048586",
            "auto.offset.reset": "largest",
            "auto.commit.enable": "false"
        },
        "feed" : "druidtest",
        "parser" : {
            "timestampSpec" : {
                "column" : "utcdt",
                "format" : "iso"
            },
            "data" : {
                "format" : "json"
            },
            "dimensionExclusions" : [
                "wp"
            ]
        }
    },
    "plumber" : {
        "type" : "realtime",
        "windowPeriod" : "PT10m",
        "segmentGranularity":"hour",
        "basePersistDirectory" : "/tmp/realtime/basePersist",
        "rejectionPolicyFactory": {
            "type": "messageTime"
        }
    }
```
