SELECT COUNT(cast( col2 as CHAR(2) )) OVER(PARTITION BY cast( col2 as CHAR(2)) ORDER BY cast( col0 as INT )) FROM "fewRowsAllData.parquet"
