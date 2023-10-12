WITH v1 ( a, b, c, d ) AS
(
    SELECT col0, col8, MAX(MIN(col8)) over (PARTITION BY col7 ORDER BY col8) as max_col8, col7 from "allTypsUniq.parquet" GROUP BY col0,col7,col8
)
SELECT a, b, c, d from ( SELECT * from v1 ) where c > 'IN' GROUP BY a,b,c,d ORDER BY a,b,c,d
