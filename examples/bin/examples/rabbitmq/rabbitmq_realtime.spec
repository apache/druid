[{
    "schema" : {
        "dataSource":"rabbitmqtest",
        "aggregators":[
            {"type":"count", "name":"impressions"},
            {"type":"doubleSum","name":"wp","fieldName":"wp"}
        ],
        "indexGranularity":"minute",
        "shardSpec" : { "type": "none" }
    },
    "config" : {
        "maxRowsInMemory" : 500000,
        "intermediatePersistPeriod" : "PT1m"
    },
    "firehose" : {
        "type" : "rabbitmq",
        "connection" : {
            "host": "localhost",
            "username": "test-dude",
            "password": "word-dude",
            "virtualHost": "test-vhost"
        },
        "config" : {
            "exchange": "test-exchange",
            "queue" : "druidtest",
            "routingKey": "#",
            "durable": "true",
            "exclusive": "false",
            "autoDelete": "false",

            "maxRetries": "10",
            "retryIntervalSeconds": "1",
            "maxDurationSeconds": "300"
        },
        "parser" : {
            "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
            "data" : { "format" : "json" },
            "dimensionExclusions" : ["wp"]
        }
    },
    "plumber" : {
        "type" : "realtime",
        "windowPeriod" : "PT5m",
        "segmentGranularity":"hour",
        "basePersistDirectory" : "/tmp/realtime/basePersist",
        "rejectionPolicy": { "type": "test" }
    }
}]
