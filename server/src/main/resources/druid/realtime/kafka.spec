[
  {
    "dataSchema" : {
      "dataSource" : "druid-sla",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "column" : "timestamp",
            "format" : "auto"
          },
          "dimensionsSpec" : {
            "dimensions": ["to","from"],
            "dimensionExclusions" : [],
            "spatialDimensions" : []
          }
        }
      },
      "metricsSpec" : [],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "MINUTE",
        "queryGranularity" : "MINUTE"
      }
    },
    "ioConfig" : {
      "type" : "realtime",
      "firehose": {
        "type": "kafka-0.8",
        "consumerProps": {
          "zookeeper.connect": "localhost:2181",
          "zookeeper.connection.timeout.ms" : "15000",
          "zookeeper.session.timeout.ms" : "15000",
          "zookeeper.sync.time.ms" : "5000",
          "group.id": "druid-sla",
          "fetch.message.max.bytes" : "1048586",
          "auto.offset.reset": "largest",
          "auto.commit.enable": "false"
        },
        "feed": "druid-sla"
      },
      "plumber": {
        "type": "realtime"
      }
    },
    "tuningConfig": {
      "type" : "realtime",
      "maxRowsInMemory": 10,
      "intermediatePersistPeriod": "PT30s",
      "windowPeriod": "PT30s",
      "basePersistDirectory": "\/Volumes\/data\/vmshare\/druid-test\/data"
    }
  }
]