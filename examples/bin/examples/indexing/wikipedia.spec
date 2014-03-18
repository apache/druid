[
  {
    "schema": {
      "dataSource": "wikipedia",
      "aggregators" : [{
         "type" : "count",
         "name" : "count"
        }, {
         "type" : "doubleSum",
         "name" : "added",
         "fieldName" : "added"
        }, {
         "type" : "doubleSum",
         "name" : "deleted",
         "fieldName" : "deleted"
        }, {
         "type" : "doubleSum",
         "name" : "delta",
         "fieldName" : "delta"
      }],
      "indexGranularity": "none"
    },
    "config": {
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT10m"
    },
    "firehose": {
      "type": "kafka-0.7.2",
      "consumerProps": {
        "zk.connect": "localhost:2181",
        "zk.connectiontimeout.ms": "15000",
        "zk.sessiontimeout.ms": "15000",
        "zk.synctime.ms": "5000",
        "groupid": "druid-example",
        "fetch.size": "1048586",
        "autooffset.reset": "largest",
        "autocommit.enable": "false"
      },
      "feed": "wikipedia",
      "parser": {
        "timestampSpec": {
          "column": "timestamp"
        },
        "data": {
          "format": "json",
          "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
        }
      }
    },
    "plumber": {
      "type": "realtime",
      "windowPeriod": "PT10m",
      "segmentGranularity": "hour",
      "basePersistDirectory": "\/tmp\/realtime\/basePersist",
      "rejectionPolicy": {
        "type": "test"
      }
    }
  }
]