REPLACE INTO "%%REINDEX_DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (
  SELECT *
  FROM "%%DATASOURCE%%"
  WHERE TIMESTAMP '2013-08-31' <= "__time" AND "__time" < TIMESTAMP '2013-09-01'
)
SELECT
  "__time",
  "continent",
  SUM("added") AS "added",
  SUM("deleted") AS "deleted",
  SUM("delta") AS "delta",
  EARLIEST("first_user", 128) AS "first_user",
  LATEST("last_user", 128) AS "last_user"
FROM "source"
GROUP BY 1, 2
PARTITIONED BY DAY