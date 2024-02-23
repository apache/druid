SELECT MIN(cast( col7 as DOUBLE )) OVER(PARTITION BY cast( col2 as VARCHAR(52)) ORDER BY cast( col3 as VARCHAR(52) )) FROM "fewRowsAllData.parquet"
