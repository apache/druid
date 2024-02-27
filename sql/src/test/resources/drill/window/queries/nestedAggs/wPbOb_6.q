SELECT c2, AVG(c1), AVG(c1) over (partition by c2), AVG(SUM(c1)) OVER (partition by c2) FROM "tblWnulls.parquet" GROUP BY c1,c2
