SELECT c2, MIN(MIN(c2)) OVER( ORDER BY c2 ) min_c2 FROM "tblWnulls.parquet" GROUP BY c2
