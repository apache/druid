[{
  "schema" : { "dataSource":"appevents",
               "aggregators":[ {"type":"count", "name":"events"}
	       		       ],
               "indexGranularity":"minute"
	       },
  "config" : { "maxRowsInMemory" : 500000,
               "intermediatePersistPeriod" : "PT1m" },
  "firehose" : { "type" : "kafka-0.6.3",
                 "consumerProps" : { "zk.connect" : "localhost:2181",
                                     "zk.connectiontimeout.ms" : "15000",
                                     "zk.sessiontimeout.ms" : "15000",
                                     "zk.synctime.ms" : "5000",
 				     "groupid" : "consumer-group",
                                     "fetch.size" : "14856",
                                     "autooffset.reset" : "largest",
                                     "autocommit.enable" : "false" },
                 "feed" : "campaigns_01",
                 "parser" : { "timestampSpec" : { "column" : "event_timestamp", "format" : "millis" },
                              "data" : { "format" : "json" }
                 } },
  "plumber" : { "type" : "realtime",
                "windowPeriod" : "PT1m",
                "segmentGranularity":"hour",
                "basePersistDirectory" : "/tmp/realtime/eval2BasePersist" }
}]

