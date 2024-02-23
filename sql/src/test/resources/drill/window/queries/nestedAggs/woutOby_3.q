SELECT c2, MAX(MIN(c1)) OVER ( PARTITION BY c2 ) FROM "tblWnulls.parquet" GROUP BY c2
