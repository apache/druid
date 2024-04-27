SELECT c2, MAX(SUM(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
