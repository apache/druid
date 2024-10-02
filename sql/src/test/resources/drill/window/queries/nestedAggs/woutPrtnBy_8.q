SELECT c2, MIN(MAX(c2)) OVER( ORDER BY c2 ) max_c2 FROM "tblWnulls.parquet" GROUP BY c2
