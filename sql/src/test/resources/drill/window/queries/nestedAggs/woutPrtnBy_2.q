SELECT c2, MIN(MAX(c1)) OVER( ORDER BY c2 ) min_mx_c1 FROM "tblWnulls.parquet" GROUP BY c2
