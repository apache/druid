REPLACE INTO "%%REINDEX_DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (
  SELECT *
  FROM "%%DATASOURCE%%"
  WHERE TIMESTAMP '2013-08-31' <= "__time" AND "__time" < TIMESTAMP '2013-09-01'
)
SELECT
  "__time",
  "page",
  "language",
  "user",
  "unpatrolled",
  "newPage",
  "anonymous",
  "namespace",
  "country",
  "region",
  "city",
  SUM("added") AS "added",
  SUM("deleted") AS "deleted",
  SUM("delta") AS "delta"
FROM "source"
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
PARTITIONED BY DAY