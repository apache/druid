SELECT c2, MIN(COUNT(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
