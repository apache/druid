[{
    "schema": {
        "dataSource": "randseq",
        "aggregators": [
            {"type": "count", "name": "events"},
            {"type": "doubleSum", "name": "outColumn", "fieldName": "inColumn"}
        ],
        "indexGranularity": "minute",
        "shardSpec": {"type": "none"}
    },

    "config": {
        "maxRowsInMemory": 50000,
        "intermediatePersistPeriod": "PT1m"
    },

    "firehose" : { "type" : "rabbitmq",
                   "consumerProps" : { "username": "test-dude",
                                       "password": "test-word",
                                       "virtualHost": "test-vhost",
                                       "host": "localhost"
                                     },
                 "queue" : "druidtest",
                 "exchange": "test-exchange",
                 "routingKey": "#",
                 "parser" : { "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
                              "data" : { "format" : "json" },
                              "dimensionExclusions" : ["wp"] } },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "PT5m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "/tmp/example/rand_realtime/basePersist"
    }
}]
