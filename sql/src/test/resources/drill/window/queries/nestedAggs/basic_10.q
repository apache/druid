SELECT 
    col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, 
    MIN(col9) over(PARTITION BY col7 ORDER BY col8) 
FROM "allTypsUniq.parquet" 
GROUP BY col0,col1,col2,col3,col4,col5,col6,col7,col8,col9
