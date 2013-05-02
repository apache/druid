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
        "intermediatePersistPeriod": "PT10m"
    },

    "firehose": {
        "type": "rand",
        "sleepUsec": 100000,
        "maxGeneratedRows": 5000000,
        "seed": 0,
        "nTokens": 255,
        "nPerSleep": 3
    },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "PT5m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "/tmp/example/rand_realtime/basePersist"
    }
}]
