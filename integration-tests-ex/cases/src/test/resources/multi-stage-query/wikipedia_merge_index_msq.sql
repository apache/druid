REPLACE INTO "%%DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"local","baseDir":"/resources/data/batch_index/json","filter":"wikipedia_index_data*"}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"continent","type":"string"},{"name":"added","type":"double"},{"name":"deleted","type":"double"},{"name":"delta","type":"double"},{"name":"user","type":"string"}]'
  )
))
SELECT
  TIME_FLOOR(CASE WHEN CAST("timestamp" AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) ELSE TIME_PARSE("timestamp") END, 'P1D') AS __time,
  "continent",
  COUNT(*) AS "count",
  SUM("added") AS "added",
  SUM("deleted") AS "deleted",
  SUM("delta") AS "delta",
  EARLIEST("user", 128) AS "first_user",
  LATEST("user", 128) AS "last_user"
FROM "source"
GROUP BY 1, 2
PARTITIONED BY DAY