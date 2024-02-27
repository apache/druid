SELECT AVG(cast( col7 as DOUBLE )) OVER(PARTITION BY cast( col4 as TIMESTAMP) ORDER BY cast( col5 as DATE )) FROM "fewRowsAllData.parquet"
