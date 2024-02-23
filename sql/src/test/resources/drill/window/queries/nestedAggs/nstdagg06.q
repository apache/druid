SELECT c2, AVG(AVG(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
