SELECT c2, SUM(MIN(c1)) OVER() FROM "tblWnulls.parquet" GROUP BY c2
