dec
{
  "queryType" : "scan",
  "dataSource" : {
    "type" : "table",
    "name" : "numfoo"
  },
  "intervals" : {
    "type" : "intervals",
    "intervals" : [ "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z" ]
  },
  "virtualColumns" : [ {
    "type" : "expression",
    "name" : "v0",
    "expression" : "mv_to_array(\"dim3\")",
    "outputType" : "ARRAY<STRING>"
  } ],
  "resultFormat" : "compactedList",
  "filter" : {
    "type" : "in",
    "dimension" : "m1",
    "values" : [ "4.0", "5.0" ]
  },
  "columns" : [ "dim3", "v0" ],
  "context" : {
    "debug" : true,
    "defaultTimeout" : 300000,
    "maxScatterGatherBytes" : 9223372036854775807,
    "plannerStrategy" : "DECOUPLED",
    "sqlCurrentTimestamp" : "2000-01-01T00:00:00Z",
    "sqlQueryId" : "dummy",
    "sqlStringifyArrays" : false,
    "vectorSize" : 2,
    "vectorize" : "force",
    "vectorizeVirtualColumns" : "force"
  },
  "columnTypes" : [ "STRING", "ARRAY<STRING>" ],
  "granularity" : {
    "type" : "all"
  },
  "legacy" : false
}
