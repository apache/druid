REPLACE INTO "%%REINDEX_DATASOURCE%%" OVERWRITE ALL
WITH "source" AS (
  SELECT *
  FROM "%%DATASOURCE%%"
  WHERE TIMESTAMP '2013-08-31' <= "__time" AND "__time" < TIMESTAMP '2013-09-01'
)
SELECT
  "__time",
  "language",
  "user",
  "unpatrolled",
  "page",
  "page" AS "newPage",
  "anonymous",
  "namespace",
  "country",
  "region",
  concat('city-', city) AS "city",
  SUM("added") AS "added",
  SUM("triple-added") AS "triple-added",
  SUM("triple-added" + 1) AS "one-plus-triple-added",
  SUM("deleted") AS "deleted",
  SUM("deleted" * 2) AS "double-deleted",
  SUM("delta" / 2) AS "delta"
FROM "source"
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
PARTITIONED BY DAY