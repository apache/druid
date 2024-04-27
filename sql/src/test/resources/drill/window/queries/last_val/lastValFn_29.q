SELECT * FROM (SELECT col7 , LAST_VALUE(col7) OVER(PARTITION BY col7 ORDER BY col7) LAST_VALUE_col7 FROM "allTypsUniq.parquet" ) sub_query order by LAST_VALUE_col7
