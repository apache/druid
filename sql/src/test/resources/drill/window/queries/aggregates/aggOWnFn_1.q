 SELECT col1 ,col2 ,LEAD(col2) OVER windw
 FROM (
  SELECT
    col0 , col1 , col2, col7 ,
    NTILE(3) over windw
  FROM "allTypsUniq.parquet"
  WINDOW windw AS (PARTITION BY col7 ORDER BY col0)
 ) sub_query
 WINDOW windw as (PARTITION BY col7 ORDER BY col0)


-- LogicalProject(LEAD) -> LogicalScan -> LogicalProject(NTILE) -> LogicalScan -> TableReference