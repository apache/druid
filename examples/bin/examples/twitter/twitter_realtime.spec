[
  {
    "dataSchema": {
      "dataSource": "twitterstream",
      "parser": {
        "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "utcdt",
            "format": "iso"
          },
          "dimensionsSpec": {
            "dimensions": [
              
            ],
            "dimensionExclusions": [
              
            ],
            "spatialDimensions": [
              
            ]
          }
        }
      },
      "metricsSpec": [
        {
          "type": "count",
          "name": "tweets"
        },
        {
          "type": "doubleSum",
          "fieldName": "follower_count",
          "name": "total_follower_count"
        },
        {
          "type": "doubleSum",
          "fieldName": "retweet_count",
          "name": "total_retweet_count"
        },
        {
          "type": "doubleSum",
          "fieldName": "friends_count",
          "name": "total_friends_count"
        },
        {
          "type": "doubleSum",
          "fieldName": "statuses_count",
          "name": "total_statuses_count"
        },
        {
          "type": "min",
          "fieldName": "follower_count",
          "name": "min_follower_count"
        },
        {
          "type": "max",
          "fieldName": "follower_count",
          "name": "max_follower_count"
        },
        {
          "type": "min",
          "fieldName": "friends_count",
          "name": "min_friends_count"
        },
        {
          "type": "max",
          "fieldName": "friends_count",
          "name": "max_friends_count"
        },
        {
          "type": "min",
          "fieldName": "statuses_count",
          "name": "min_statuses_count"
        },
        {
          "type": "max",
          "fieldName": "statuses_count",
          "name": "max_statuses_count"
        },
        {
          "type": "min",
          "fieldName": "retweet_count",
          "name": "min_retweet_count"
        },
        {
          "type": "max",
          "fieldName": "retweet_count",
          "name": "max_retweet_count"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE"
      }
    },
    "ioConfig": {
      "type": "realtime",
      "firehose": {
        "type": "twitzer",
        "maxEventCount": 500000,
        "maxRunMinutes": 120
      },
      "plumber": {
        "type": "realtime"
      }
    },
    "tuningConfig": {
      "type": "realtime",
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT2m",
      "windowPeriod": "PT3m",
      "basePersistDirectory": "\/tmp\/realtime\/basePersist",
      "rejectionPolicy": {
        "type": "messageTime"
      }
    }
  }
]
