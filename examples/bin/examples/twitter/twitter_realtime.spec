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
              "text",
              "htags",
              "contributors",
              "lat",
              "lon",
              "source",
              "retweet",
              "retweet_count",
              "originator_screen_name",
              "originator_follower_count",
              "originator_friends_count",
              "originator_verified",
              "follower_count",
              "friends_count",
              "lang",
              "utc_offset",
              "statuses_count",
              "user_id",
              "screen_name",
              "location",
              "verified",
              "ts"
            ],
            "dimensionExclusions": [

            ],
            "spatialDimensions": [
              {
                "dimName": "geo",
                "dims": [
                  "lat",
                  "lon"
                ]
              }
            ]
          }
        }
      },
      "metricsSpec": [
        {
          "name": "tweets",
          "type": "count"
        },
        {
          "fieldName": "follower_count",
          "name": "total_follower_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "retweet_count",
          "name": "total_retweet_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "friends_count",
          "name": "total_friends_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "statuses_count",
          "name": "total_statuses_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "originator_follower_count",
          "name": "total_originator_follower_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "originator_friends_count",
          "name": "total_originator_friends_count",
          "type": "doubleSum"
        },
        {
          "fieldName": "text",
          "name": "text_hll",
          "type": "hyperUnique"
        },
        {
          "fieldName": "user_id",
          "name": "user_id_hll",
          "type": "hyperUnique"
        },
        {
          "fieldName": "contributors",
          "name": "contributors_hll",
          "type": "hyperUnique"
        },
        {
          "fieldName": "htags",
          "name": "htags_hll",
          "type": "hyperUnique"
        },
        {
          "fieldName": "follower_count",
          "name": "min_follower_count",
          "type": "min"
        },
        {
          "fieldName": "follower_count",
          "name": "max_follower_count",
          "type": "max"
        },
        {
          "fieldName": "friends_count",
          "name": "min_friends_count",
          "type": "min"
        },
        {
          "fieldName": "friends_count",
          "name": "max_friends_count",
          "type": "max"
        },
        {
          "fieldName": "statuses_count",
          "name": "min_statuses_count",
          "type": "min"
        },
        {
          "fieldName": "statuses_count",
          "name": "max_statuses_count",
          "type": "max"
        },
        {
          "fieldName": "retweet_count",
          "name": "min_retweet_count",
          "type": "min"
        },
        {
          "fieldName": "retweet_count",
          "name": "max_retweet_count",
          "type": "max"
        },
        {
          "fieldName": "originator_follower_count",
          "name": "min_originator_follower_count",
          "type": "min"
        },
        {
          "fieldName": "originator_follower_count",
          "name": "max_originator_follower_count",
          "type": "max"
        },
        {
          "fieldName": "originator_friends_count",
          "name": "min_originator_friends_count",
          "type": "min"
        },
        {
          "fieldName": "originator_friends_count",
          "name": "max_originator_friends_count",
          "type": "max"
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
