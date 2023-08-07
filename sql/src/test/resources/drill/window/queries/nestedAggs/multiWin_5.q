SELECT col7, AVG(SUM(avg_sum_c0)) OVER ( PARTITION BY col7 ORDER BY col8 )
FROM
( 
    SELECT 
    col7, AVG(SUM(col0)) OVER ( PARTITION BY col7 ORDER BY col8 ) avg_sum_c0 , col8
    FROM "allTypsUniq.parquet" 
    GROUP BY col7,col8 
)
GROUP BY col7,col8
