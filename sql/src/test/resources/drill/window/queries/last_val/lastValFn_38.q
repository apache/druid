SELECT * FROM (SELECT col7, col9 , LAST_VALUE(col9) OVER(PARTITION BY col7 ORDER BY col9) LAST_VALUE_col9 FROM "allTypsUniq.parquet") sub_query order by LAST_VALUE_col9
