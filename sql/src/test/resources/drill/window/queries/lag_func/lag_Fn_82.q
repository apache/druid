SELECT col7 , col0 , col1, col2, col3, col5, col6, col7 , col8 , col9 , LAG(col0) OVER(PARTITION BY col7 ORDER BY col0) LAG_col0 FROM "allTypsUniq.parquet"
