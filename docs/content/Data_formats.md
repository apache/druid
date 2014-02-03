---
layout: doc_page
---
Data Formats for Ingestion
==========================

Druid can ingest data in JSON, CSV, or TSV. While most examples in the documentation use data in JSON format, it is not difficult to configure Druid to ingest CSV or TSV data.

## Formatting the Data
The following are three samples of the data used in the [Wikipedia example](Tutorial:-Loading-Your-Data-Part-1.html).

_JSON_

```json
{"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
{"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
{"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
{"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
{"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "cancer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
```

_CSV_

```
2013-08-31T01:02:33Z,"Gypsy Danger","en","nuclear","true","true","false","false","article","North America","United States","Bay Area","San Francisco",57,200,-143
2013-08-31T03:32:45Z,"Striker Eureka","en","speed","false","true","true","false","wikipedia","Australia","Australia","Cantebury","Syndey",459,129,330
2013-08-31T07:11:21Z,"Cherno Alpha","ru","masterYi","false","true","true","false","article","Asia","Russia","Oblast","Moscow",123,12,111
2013-08-31T11:58:39Z,"Crimson Typhoon","zh","triplets","true","false","true","false","wikipedia","Asia","China","Shanxi","Taiyuan",905,5,900
2013-08-31T12:41:27Z,"Coyote Tango","ja","cancer","true","false","true","false","wikipedia","Asia","Japan","Kanto","Tokyo",1,10,-9
```

_TSV_

```
2013-08-31T01:02:33Z	"Gypsy Danger"	"en"	"nuclear"	"true"	"true"	"false"	"false"	"article"	"North America"	"United States"	"Bay Area"	"San Francisco"	57	200	-143
2013-08-31T03:32:45Z	"Striker Eureka"	"en"	"speed"	"false"	"true"	"true"	"false"	"wikipedia"	"Australia"	"Australia"	"Cantebury"	"Syndey"	459	129	330
2013-08-31T07:11:21Z	"Cherno Alpha"	"ru"	"masterYi"	"false"	"true"	"true"	"false"	"article"	"Asia"	"Russia"	"Oblast"	"Moscow"	123	12	111
2013-08-31T11:58:39Z	"Crimson Typhoon"	"zh"	"triplets"	"true"	"false"	"true"	"false"	"wikipedia"	"Asia"	"China"	"Shanxi"	"Taiyuan"	905	5	900
2013-08-31T12:41:27Z	"Coyote Tango"	"ja"	"cancer"	"true"	"false"	"true"	"false"	"wikipedia"	"Asia"	"Japan"	"Kanto"	"Tokyo"	1	10	-9
```

Note that the CSV and TSV data do not contain column heads. This becomes important when you specify the data for ingesting.

## Configuring Ingestion For the Indexing Service
If you use the [indexing service](Indexing-Service.html) for ingesting the data, a [task](Tasks.html) must be configured and submitted. Tasks are configured with a JSON object which, among other things, specifies the data source and type. In the Wikipedia example, JSON data was read from a local file. The task spec contains a firehose element to specify this:

```json
    "firehose" : {
      "type" : "local",
      "baseDir" : "examples/indexing",
      "filter" : "wikipedia_data.json",
      "parser" : {
        "timestampSpec" : {
          "column" : "timestamp"
        },
        "data" : {
          "format" : "json",
          "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
        }
      }
    }
```

Specified here are the location of the datafile, the timestamp column, the format of the data, and the columns that will become dimensions in Druid.

Since the CSV data does not contain the column names, they will have to be added before that data can be processed:

```json
    "firehose" : {
      "type" : "local",
      "baseDir" : "examples/indexing/",
      "filter" : "wikipedia_data.csv",
      "parser" : {
        "timestampSpec" : {
          "column" : "timestamp"
        },
        "data" : {
          "type" : "csv",
          "columns" : ["timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
          "dimensions" : ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"]
        }
      }
    }
```

Note also that the filename extension and the data type were changed to "csv". For the TSV data, the same changes are made but with "tsv" for the filename extension and the data type.

