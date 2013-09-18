[{
    "schema": {
        "dataSource": "wikipedia",
        "aggregators": [
            {"type": "count", "name": "count"},
            {"type": "longSum", "fieldName": "added", "name": "added"},
            {"type": "longSum", "fieldName": "deleted", "name": "deleted"},
            {"type": "longSum", "fieldName": "delta", "name": "delta"}
        ],
        "indexGranularity": "minute",
        "shardSpec": {"type": "none"}
	    },

    "config": {
        "maxRowsInMemory": 50000,
        "intermediatePersistPeriod": "PT2m"
    },

    "firehose": {
        "type": "irc",
        "nick": "wiki1234567890",
        "host": "irc.wikimedia.org",
        "channels": [
          "#en.wikipedia",
          "#fr.wikipedia",
          "#de.wikipedia",
          "#ja.wikipedia"
        ],
        "decoder":  {
          "type": "wikipedia",
          "namespaces": {
            "#en.wikipedia": {
              "": "main",
              "Category": "category",
              "$1 talk": "project talk",
              "Template talk": "template talk",
              "Help talk": "help talk",
              "Media": "media",
              "MediaWiki talk": "mediawiki talk",
              "File talk": "file talk",
              "MediaWiki": "mediawiki",
              "User": "user",
              "File": "file",
              "User talk": "user talk",
              "Template": "template",
              "Help": "help",
              "Special": "special",
              "Talk": "talk",
              "Category talk": "category talk"
            }
          }
        },
        "timeDimension":"timestamp",
	"timeFormat":"iso"
    },

    "plumber": {
        "type": "realtime",
        "windowPeriod": "PT3m",
        "segmentGranularity": "hour",
        "basePersistDirectory": "/tmp/example/wikipedia/basePersist"
    }
}]
