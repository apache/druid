SELECT c2, AVG(SUM(c1)) OVER( ORDER BY c2 ) avg_sum_c1 FROM "tblWnulls.parquet" GROUP BY c2
