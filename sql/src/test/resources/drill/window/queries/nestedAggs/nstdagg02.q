SELECT c2, AVG(COUNT(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
