with v1 (a,b) AS
 (
 SELECT col7, count(col0) over (PARTITION BY col7 ORDER BY col0) count_col0 from "allTypsUniq.parquet"
 )
SELECT count(b) over (PARTITION BY a ORDER BY a) from v1
