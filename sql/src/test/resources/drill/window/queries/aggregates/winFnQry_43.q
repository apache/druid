SELECT COUNT(cast( col2 as CHAR(2) )) OVER(PARTITION BY cast( col8 as TIME) ORDER BY cast( col3 as VARCHAR(52) )) FROM "fewRowsAllData.parquet"
