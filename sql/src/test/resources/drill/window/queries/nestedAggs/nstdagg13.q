SELECT c2, MAX(COUNT(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
