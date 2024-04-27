SELECT c2, COUNT(MIN(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
