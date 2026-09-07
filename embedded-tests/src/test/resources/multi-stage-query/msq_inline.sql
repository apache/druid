REPLACE INTO "%%DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"timestamp\": \"2013-08-31T01:02:33Z\", \"values\": [0,1,2,3,4] }"}',
    '{"type":"json","flattenSpec":{"useFieldDiscovery":true,"fields":[{"type":"path","name":"len","expr":"$.values.length()"},{"type":"path","name":"min","expr":"$.values.min()"},{"type":"path","name":"max","expr":"$.values.max()"},{"type":"path","name":"sum","expr":"$.values.sum()"}]}}',
    '[{"name":"timestamp","type":"string"},{"name":"len","type":"long"},{"name":"min","type":"long"},{"name":"max","type":"long"},{"name":"sum","type":"long"}]'
  )
))
SELECT
  TIME_PARSE("timestamp") AS __time,
  "len",
  "min",
  "max",
  "sum"
FROM "source"
GROUP BY 1, 2, 3, 4, 5
PARTITIONED BY HOUR