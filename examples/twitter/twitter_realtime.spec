[{
  "schema" : { "dataSource":"twitterstream",
               "aggregators":[ 
                       {"type":"count", "name":"events"},
	       		       {"type":"doubleSum","fieldName":"followerCount","name":"totalFollowerCount"},
	       		       {"type":"doubleSum","fieldName":"retweetCount","name":"totalRetweetCount"},
	       		       {"type":"doubleSum","fieldName":"friendsCount","name":"totalFriendsCount"},
	       		       {"type":"doubleSum","fieldName":"statusesCount","name":"totalStatusesCount"},

	       		       {"type":"min","fieldName":"followerCount","name":"minFollowerCount"},
	       		       {"type":"max","fieldName":"followerCount","name":"maxFollowerCount"},

	       		       {"type":"min","fieldName":"friendsCount","name":"minFriendsCount"},
	       		       {"type":"max","fieldName":"friendsCount","name":"maxFriendsCount"},

	       		       {"type":"min","fieldName":"statusesCount","name":"minStatusesCount"},
	       		       {"type":"max","fieldName":"statusesCount","name":"maxStatusesCount"},

	       		       {"type":"min","fieldName":"retweetCount","name":"minRetweetCount"},
	       		       {"type":"max","fieldName":"retweetCount","name":"maxRetweetCount"}

                              ],
               "indexGranularity":"minute",
	       "shardSpec" : { "type": "none" } },
  "config" : { "maxRowsInMemory" : 50000,
               "intermediatePersistPeriod" : "PT2m" },
 
  "firehose" : { "type" : "twitzer",
                 "maxEventCount": 10000,
                 "maxRunMinutes" : 5
                },

  "plumber" : { "type" : "realtime",
                "windowPeriod" : "PT3m",
                "segmentGranularity":"hour",
                "basePersistDirectory" : "/tmp/twitter_realtime/basePersist" }
}]
