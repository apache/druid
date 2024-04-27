SELECT * FROM (SELECT col7 , LAG(col1) OVER(PARTITION BY col7 ORDER BY col1) LAG_col1 FROM "allTypsUniq.parquet") sub_query WHERE LAG_col1 is null
