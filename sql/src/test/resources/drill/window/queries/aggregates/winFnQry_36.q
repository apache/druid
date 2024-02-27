SELECT AVG(cast( col1 as BIGINT )) OVER(PARTITION BY cast( col4 as TIMESTAMP) ORDER BY cast( col5 as DATE )) FROM "fewRowsAllData.parquet"
