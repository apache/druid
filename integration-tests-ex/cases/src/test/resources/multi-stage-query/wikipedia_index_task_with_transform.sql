REPLACE INTO "%%DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"local","baseDir":"/resources/data/batch_index/json","filter":"wikipedia_index_data*"}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"page","type":"string"},{"name":"language","type":"string"},{"name":"user","type":"string"},{"name":"unpatrolled","type":"string"},{"name":"robot","type":"string"},{"name":"anonymous","type":"string"},{"name":"namespace","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"region","type":"string"},{"name":"city","type":"string"},{"name":"added","type":"double"},{"name":"triple-added","type":"double"},{"name":"deleted","type":"double"},{"name":"delta","type":"double"}]'
  )
))
SELECT
  TIME_FLOOR(CASE WHEN CAST("timestamp" AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) ELSE TIME_PARSE("timestamp") END, 'PT1S') AS __time,
  "page",
  concat('language-', "language") AS "language",
  "user",
  "unpatrolled",
  "robot",
  "anonymous",
  "namespace",
  "continent",
  "country",
  "region",
  "city",
  COUNT(*) AS "count",
  SUM("added") AS "added",
  SUM("added")*3 AS "triple-added",
  SUM("deleted") AS "deleted",
  SUM("delta") AS "delta",
  APPROX_COUNT_DISTINCT_DS_THETA("user") AS "thetaSketch",
  DS_QUANTILES_SKETCH("delta") AS "quantilesDoublesSketch",
  APPROX_COUNT_DISTINCT_DS_HLL("user") AS "HLLSketchBuild"
FROM "source"
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
PARTITIONED BY DAY