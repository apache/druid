SELECT c2, COUNT(AVG(c1)) OVER( ORDER BY c2 ) count_avg_c1 FROM "tblWnulls.parquet" GROUP BY c2
