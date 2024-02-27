SELECT COUNT(cast( col2 as CHAR(2) )) OVER(PARTITION BY cast( col5 as DATE) ORDER BY cast( col4 as TIMESTAMP )) FROM "fewRowsAllData.parquet"
