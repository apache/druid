SELECT * FROM (SELECT LAST_VALUE(col3) OVER(PARTITION BY col7 ORDER BY col0,col3) LAST_VALUE_col3 FROM "allTypsUniq.parquet") sub_query WHERE LAST_VALUE_col3 is NOT null
