SELECT LEAD_col2 FROM ( SELECT LEAD(col2) OVER( PARTITION BY col3 ORDER BY col1 desc nulls LAST ) LEAD_col2 FROM "fewRowsAllData.parquet") sub_query WHERE LEAD_col2 IS NOT null
