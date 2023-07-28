SELECT c2, SUM(AVG(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
