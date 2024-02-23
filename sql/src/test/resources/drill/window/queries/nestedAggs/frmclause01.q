SELECT 
    col7, 
    AVG(SUM(col0)) OVER ( PARTITION BY col7 ORDER BY col8 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) avg_sum_c0 , 
    col8 
FROM "allTypsUniq.parquet" 
GROUP BY col7,col8
