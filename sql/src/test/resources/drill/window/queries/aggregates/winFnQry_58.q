SELECT MIN(cast( col7 as DOUBLE )) OVER(PARTITION BY cast( col7 as DOUBLE) ORDER BY cast( col0 as INT )) FROM "fewRowsAllData.parquet"
