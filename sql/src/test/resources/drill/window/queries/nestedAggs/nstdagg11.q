SELECT c2, MAX(MIN(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
