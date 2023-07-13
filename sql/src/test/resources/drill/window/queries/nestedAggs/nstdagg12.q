SELECT c2, MAX(MAX(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
