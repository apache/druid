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
        "consumerProps" : {
            "host": "localhost",
            "username": "test-dude",
            "password": "test-word",
            "virtualHost": "test-vhost",
            "durable": "true",
            "exclusive": "false",
            "autoDelete": "false",
            "autoAck": "false"
        },
        "exchange": "test-exchange",
        "queue" : "druidtest",
        "routingKey": "#",
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
        "rejectionPolicy": { "type": "messageTime" }
    }
}]