REPLACE INTO "%%DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"inline","data":"{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"C\",\"dimB\":\"F\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"C\",\"dimB\":\"J\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"Z\",\"dimB\":\"S\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"Z\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"J\",\"dimB\":\"R\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"T\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimA\":\"H\",\"dimB\":\"X\",\"metA\":1}\n{\"time\":\"2015-09-12T00:46:58.771Z\",\"dimC\":\"A\",\"dimB\":\"X\",\"metA\":1}\n"}',
    '{"type":"json"}',
    '[{"name":"time","type":"string"},{"name":"dimB","type":"string"},{"name":"dimA","type":"string"},{"name":"dimC","type":"string"},{"name":"dimD","type":"string"},{"name":"dimE","type":"string"},{"name":"dimF","type":"string"},{"name":"metA","type":"long"}]'
  )
))
SELECT
  TIME_FLOOR(TIME_PARSE("time"), 'PT1H') AS __time,
  "dimB",
  "dimA",
  "dimC",
  "dimD",
  "dimE",
  "dimF",
  COUNT(*) AS "count",
  SUM("metA") AS "sum_metA"
FROM "source"
GROUP BY 1, 2, 3, 4, 5, 6, 7
PARTITIONED BY HOUR