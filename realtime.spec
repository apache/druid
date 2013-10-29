[{
  "schema" : { "dataSource":"druidtest",
               "aggregators":[
                  {"type":"longSum","name":"sum_wp","fieldName":"wp"},
                  {"type":"min","name":"min_wp","fieldName":"wp"},
                  {"type":"max","name":"max_wp","fieldName":"wp"},
                  {"type":"jon","name":"jon_wp","fieldName":"wp"},
				  {"type":"cardinality","name":"card_wp","fieldName":"wp"}
                ],  
               "indexGranularity":"minute",
           "shardSpec" : { "type": "none" } },
  "config" : { "maxRowsInMemory" : 500000,
               "intermediatePersistPeriod" : "PT10m" },
  "firehose" : { "type" : "kafka-0.7.2",
                 "consumerProps" : { "zk.connect" : "localhost:2181",
                                     "zk.connectiontimeout.ms" : "15000",
                                     "zk.sessiontimeout.ms" : "15000",
                                     "zk.synctime.ms" : "5000",
                                     "groupid" : "topic-pixel-local",
                                     "fetch.size" : "1048586",
                                     "autooffset.reset" : "largest",
                                     "autocommit.enable" : "false" },
                 "feed" : "druidtest",
                 "parser" : { "timestampSpec" : { "column" : "timestamp", "format" : "ruby" },
                              "data" : { "format" : "json" },
                              "dimensionExclusions" : ["wp"] } },
  "plumber" : { "type" : "realtime",
                "windowPeriod" : "PT10m",
                "segmentGranularity":"hour",
                "basePersistDirectory" : "/tmp/realtime/basePersist",
                "rejectionPolicy": {"type": "messageTime"} }

}]
