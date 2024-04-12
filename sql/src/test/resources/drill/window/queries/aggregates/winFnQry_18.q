SELECT MIN(cast( col0 as BIGINT )) OVER(PARTITION BY cast( col2 as CHAR(2)) ORDER BY cast( col0 as BIGINT )) FROM "fewRowsAllData.parquet"
