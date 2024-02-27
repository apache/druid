SELECT c2, SUM(SUM(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
