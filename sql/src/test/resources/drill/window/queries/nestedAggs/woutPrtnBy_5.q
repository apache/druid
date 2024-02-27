SELECT c2, SUM(AVG(c1)) OVER( ORDER BY c2 ) sum_avg_c1 FROM "tblWnulls.parquet" GROUP BY c2
