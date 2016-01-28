---
layout: doc_page
---
# Union All Queries
Unions multiple query results into single one. All sub queries should be the same type.

```json
 {
   "queryType": "unionAll",
   "sortOnUnion": "false",
   "queries":[
   {
     "queryType": "select",
     "dataSource": "wikipedia",
     "descending": "false",
     "dimensions":[],
     "metrics":[],
     "granularity": "all",
     "intervals": [
       "2013-01-01/2013-01-02"
     ],
     "pagingSpec":{"pagingIdentifiers": {}, "threshold":5}
   },
   {
     "queryType": "select",
     "dataSource": "wikipedia",
     "descending": "false",
     "dimensions":[],
     "metrics":[],
     "granularity": "all",
     "intervals": [
       "2014-01-01/2014-01-02"
     ],
     "pagingSpec":{"pagingIdentifiers": {}, "threshold":5}
   }
   ]
 }
```

There are several main parts to a union all query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "unionAll"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|sortOnUnion|Whether to sort and merge result. If this is false, result will be simply concatenated for one by one. Default is `false`.|no|
|queries|A JSON array of query objects. There should be at least one query object and all queries should be the same type.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

