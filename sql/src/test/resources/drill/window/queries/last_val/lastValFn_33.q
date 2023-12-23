SELECT * FROM (SELECT col7, col8 , LAST_VALUE(col8) OVER(PARTITION BY col7 ORDER BY col8) LAST_VALUE_col8 FROM "allTypsUniq.parquet" ) sub_query where LAST_VALUE_col8 IN ('CA','NE','IN','TX','GA')
