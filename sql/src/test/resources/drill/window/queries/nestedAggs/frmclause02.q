SELECT 
    col7, 
    MAX(COUNT(col1)) OVER ( PARTITION BY col7 ORDER BY col8 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) mx_count_c1 , 
    col8 
FROM "allTypsUniq.parquet" 
GROUP BY col7,col8
