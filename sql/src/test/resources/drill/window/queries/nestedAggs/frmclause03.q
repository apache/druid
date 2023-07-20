SELECT 
    col7, 
    MAX(MIN(col6)) OVER ( PARTITION BY col7 ORDER BY col8 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) mx_min_c6 , 
    col8 
FROM "allTypsUniq.parquet" 
GROUP BY col7,col8
