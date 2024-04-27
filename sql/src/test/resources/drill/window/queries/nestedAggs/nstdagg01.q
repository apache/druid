SELECT c2, AVG(SUM(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
