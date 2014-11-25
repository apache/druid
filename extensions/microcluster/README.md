A micro cluster consisting of a historical, coordinator, broker, overlord, and middle-manager node. Also tries to run zookeeper and a derby instance for metadata.

Each of the Druid services runs in its own JVM but the IO is inherited from the master JVM.

This microcluster is completely transient, and intended to have all data lost upon termination. As such, it is for example purposes only.

To run, call the `microcluster all` command with the following JVM options:
`java -server -Duser.timezone=UTC -Dfile.encoding=UTF-8 -cp 'druid_lib_path/*' io.druid.cli.Main microcluster all`

To add a mysql database (instead of the embedded Derby), use something like the following:
```
-Ddruid.metadata.storage.type=mysql
-Ddruid.metadata.storage.connector.connectURI="jdbc:mysql://localhost:3306/druid"
-Ddruid.metadata.storage.connector.user=druid
-Ddruid.metadata.storage.connector.password=diurd
```

The realtime indexer can be started against the twitter spritzer as follows
```
curl -i -X POST 'http://localhost:8090/druid/indexer/v1/task' -H 'Content-Type: application/json' -d '{
  "spec": {
    "dataSchema": {
      "dataSource": "twitterstream",
      "granularitySpec": {
        "queryGranularity": "all",
        "type": "uniform",
        "segmentGranularity": "hour"
      },
      "metricsSpec": [
        {"type": "count", "name": "tweets"},
        {"type": "doubleSum", "fieldName": "follower_count", "name": "total_follower_count"},
        {"type": "doubleSum", "fieldName": "retweet_count",  "name": "total_retweet_count" },
        {"type": "doubleSum", "fieldName": "friends_count",  "name": "total_friends_count" },
        {"type": "doubleSum", "fieldName": "statuses_count", "name": "total_statuses_count"},
        {"type": "hyperUnique", "fieldName": "text", "name": "text_hll"},
        {"type": "hyperUnique", "fieldName": "user_id", "name": "user_id_hll"},
        {"type": "hyperUnique", "fieldName": "contributors", "name": "contributors_hll"},
        {"type": "hyperUnique", "fieldName": "htags", "name": "htags_hll"},
        {"type": "min", "fieldName": "follower_count", "name": "min_follower_count"},
        {"type": "max", "fieldName": "follower_count", "name": "max_follower_count"},
        {"type": "min", "fieldName": "friends_count", "name": "min_friends_count"},
        {"type": "max", "fieldName": "friends_count", "name": "max_friends_count"},
        {"type": "min", "fieldName": "statuses_count", "name": "min_statuses_count"},
        {"type": "max", "fieldName": "statuses_count", "name": "max_statuses_count"},
        {"type": "min", "fieldName": "retweet_count", "name": "min_retweet_count"},
        {"type": "max", "fieldName": "retweet_count", "name": "max_retweet_count"}
      ],
      "parser":{
        "parseSpec":{
          "format":"json",
          "timestampSpec":{"column":"ts", "format":"millis"},
          "dimensionsSpec":{
            "dimensions":["text","htags","contributors","lat","lon","retweet_count","follower_count","friendscount","lang","utc_offset","statuses_count","user_id","ts"],
            "spatialDimensions":[{"dimName":"geo","dims":["lat","lon"]}]
          }
        }
      }
    },
      "ioConfig": {
        "type":"realtime",
        "firehose": {
          "type": "twitzer",
          "maxEventCount": 500000,
          "maxRunMinutes": 120
        }
      },
      "tuningConfig": {
        "type":"realtime",
        "intermediatePersistPeriod": "PT10m",
        "maxRowsInMemory": 500000,
        "windowPeriod": "PT10m"
      }
  },
  "type": "index_realtime"
}'

```

After a short pause you should be able to query data as follows:

```
curl -i -X POST "http://localhost:8082/druid/v2/?pretty" -H 'content-type: application/json' -d '{
  "queryType"  : "timeseries",
  "dataSource" : "twitterstream",
  "granularity" : "hour",
  "intervals": [ "1970-01-01T00:00:00.000/2019-01-03T00:00:00.000" ],
  "aggregations": [
    { "type": "count", "name": "count" },
    { "type": "hyperUnique", "name":"text_hll", "fieldName":"text_hll" },
    { "type": "hyperUnique", "name":"htag_hll", "fieldName":"htags_hll" },
    { "type": "hyperUnique", "name":"user_id_hll", "fieldName":"user_id_hll" }
  ]
}'
```