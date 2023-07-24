SELECT c2, MIN(AVG(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
