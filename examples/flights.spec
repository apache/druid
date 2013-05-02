[{
    "schema": {
        "dataSource": "flights",
        "aggregators": [
            {"type": "count", "name": "flights"},
            {"type": "longSum", "name": "Distance", "fieldName": "Distance"},
            {"type": "longSum", "name": "TaxiIn", "fieldName": "TaxiIn"},
            {"type": "longSum", "name": "TaxiOut", "fieldName": "TaxiOut"},
            {"type": "longSum", "name": "CarrierDelay", "fieldName": "CarrierDelay"},
            {"type": "longSum", "name": "WeatherDelay", "fieldName": "WeatherDelay"},
            {"type": "longSum", "name": "NASDelay", "fieldName": "NASDelay"},
            {"type": "longSum", "name": "SecurityDelay", "fieldName": "SecurityDelay"},
            {"type": "longSum", "name": "LateAircraftDelay", "fieldName": "LateAircraftDelay"},
            {"type": "longSum", "name": "ArrDelay", "fieldName": "ArrDelay"},
            {"type": "longSum", "name": "DepDelay", "fieldName": "DepDelay"},
            {"type": "longSum", "name": "CRSElapsedTime", "fieldName": "CRSElapsedTime"},
            {"type": "longSum", "name": "ActualElapsedTime", "fieldName": "ActualElapsedTime"},
            {"type": "longSum", "name": "AirTime", "fieldName": "AirTime"}
        ],
        "indexGranularity": "minute",
        "shardSpec": {"type": "none"}
    },

    "config": {
        "maxRowsInMemory": 650000,
        "intermediatePersistPeriod": "PT30m"
    },

    "firehose": {
        "type": "flights",
        "directory": "/Users/cheddar/work/third-party/sensei-ba/sensei-ba-1.5.1-SNAPSHOT/flights-data/converted/druid_in",
        "parser" : { "timestampSpec" : { "column" : "timestamp", "format" : "iso" },
                     "data" : { "format" : "json" },
                     "dimensionExclusions" : ["Distance", "TaxiIn", "TaxiOut", "CarrierDelay", "WeatherDelay",
                                              "NASDelay", "SecurityDelay", "LateAircraftDelay", "ArrDelay",
                                              "DepDelay", "CRSElapsedTime", "ActualElapsedTime", "AirTime"] }
    },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "P365d",
        "segmentGranularity": "year",
        "basePersistDirectory": "/tmp/druid/flights/basePersist",
        "rejectionPolicy": {"type": "messageTime"}
    }
}]
