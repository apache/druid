SELECT col7 , col4 , LAG(col4) OVER(PARTITION BY col7 ORDER BY col4) LAG_col4 FROM "allTypsUniq.parquet"
