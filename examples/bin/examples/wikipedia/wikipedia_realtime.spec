[{
  "dataSchema": {
    "dataSource": "wikipedia",
    "parser": {
      "type": "irc",
      "parseSpec": {
        "format": "json",
        "timestampSpec": {
          "column": "timestamp",
          "format": "iso"
        },
        "dimensionsSpec": {
          "dimensions": [
            "page",
            "language",
            "user",
            "unpatrolled",
            "newPage",
            "robot",
            "anonymous",
            "namespace",
            "continent",
            "country",
            "region",
            "city"
          ],
          "dimensionExclusions": [],
          "spatialDimensions": []
        }
      },
      "decoder": {
        "type": "wikipedia",
        "namespaces": {
          "#en.wikipedia": {
            "_empty_": "main",
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
      }
    },
    "metricsSpec": [
      {
        "type": "count",
        "name": "count"
      },
      {
        "type": "doubleSum",
        "name": "added",
        "fieldName": "added"
      },
      {
        "type": "doubleSum",
        "name": "deleted",
        "fieldName": "deleted"
      },
      {
        "type": "doubleSum",
        "name": "delta",
        "fieldName": "delta"
      }
    ],
    "granularitySpec": {
      "type": "uniform",
      "segmentGranularity": "HOUR",
      "queryGranularity": "NONE"
    }
  },
  "ioConfig": {
    "type": "realtime",
    "firehose": {
      "type": "irc",
      "host": "irc.wikimedia.org",
      "channels": [
        "#en.wikipedia",
        "#fr.wikipedia",
        "#de.wikipedia",
        "#ja.wikipedia"
      ]
    },
    "plumber": {
      "type": "realtime"
    }
  },
  "tuningConfig": {
    "type": "realtime",
    "maxRowsInMemory": 500000,
    "intermediatePersistPeriod": "PT10m",
    "windowPeriod": "PT10m",
    "basePersistDirectory": "\/tmp\/realtime\/basePersist",
    "rejectionPolicy": {
      "type": "serverTime"
    }
  }
}]

