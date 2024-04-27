SELECT c2, MIN(MAX(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
