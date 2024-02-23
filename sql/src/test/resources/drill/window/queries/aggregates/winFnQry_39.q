SELECT COUNT(cast( col3 as VARCHAR(52) )) OVER(PARTITION BY cast( col5 as DATE) ORDER BY cast( col4 as TIMESTAMP )) FROM "fewRowsAllData.parquet"
