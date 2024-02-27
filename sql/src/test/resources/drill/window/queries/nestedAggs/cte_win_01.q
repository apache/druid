WITH v1 ( a, b, c ) AS
(
    SELECT col0, col8, MAX(col8) over (PARTITION BY col7 ORDER BY col8) as MAX_col8 from "allTypsUniq.parquet"
)
SELECT a, b, c from v1 where c > 'IN' ORDER BY a,b
