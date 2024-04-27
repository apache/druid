SELECT c2, COUNT(SUM(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
