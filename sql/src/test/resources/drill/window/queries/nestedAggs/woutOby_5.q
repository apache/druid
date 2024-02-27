SELECT c2, SUM(AVG(c1)) OVER ( PARTITION BY c2 ) FROM "tblWnulls.parquet" GROUP BY c2
