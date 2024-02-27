SELECT c2, MAX(MIN(c1)) OVER( ORDER BY c2 ) mx_min_c1 FROM "tblWnulls.parquet" GROUP BY c2
