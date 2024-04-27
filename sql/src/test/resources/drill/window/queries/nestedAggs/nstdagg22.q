SELECT c2, SUM(MAX(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
