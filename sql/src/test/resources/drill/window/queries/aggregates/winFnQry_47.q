SELECT MAX(cast( col7 as DOUBLE )) OVER(PARTITION BY cast( col3 as VARCHAR(52)) ORDER BY cast( col2 as CHAR(2) )) FROM "fewRowsAllData.parquet"
