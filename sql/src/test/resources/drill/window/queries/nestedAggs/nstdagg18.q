SELECT c2, COUNT(COUNT(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
