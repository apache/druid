select c1 , c2 , lead(c2) OVER ( PARTITION BY c2 ORDER BY c1) lead_c2 FROM (SELECT c1 , c2, ntile(3) over(PARTITION BY c2 ORDER BY c1) FROM "tblWnulls.parquet")
