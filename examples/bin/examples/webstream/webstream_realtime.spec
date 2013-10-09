[{
    "schema": {
        "dataSource": "webstream",
        "aggregators": [
            {"type": "count", "name": "rows"},
            {"type": "doubleSum", "fieldName": "known_users", "name": "known_users"}
        ],
        "indexGranularity": "second",
        "shardSpec": {"type": "none"}
	    },

    "config": {
        "maxRowsInMemory": 50000,
        "intermediatePersistPeriod": "PT2m"
    },

    "firehose": {
        "type": "webstream",
        "url":"http://developer.usa.gov/1usagov",
		"renamedDimensions": {
			"g":"bitly_hash",
			"c":"country",
			"a":"user",
			"cy":"city",
			"l":"encoding_user_login",
			"hh":"short_url",
			"hc":"timestamp_hash",
			"h":"user_bitly_hash",
			"u":"url",
			"tz":"timezone",
			"t":"time",
			"r":"referring_url",
			"gr":"geo_region",
			"nk":"known_users",
			"al":"accept_language"
			},
		"timeDimension":"t",
		"timeFormat":"posix"
    },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "PT3m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "/tmp/example/usagov_realtime/basePersist"
    }
}]
