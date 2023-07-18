SELECT 
    c8 ,
    MAX(MIN(c5)) OVER(PARTITION BY c8 ORDER BY c1 RANGE BETWEEN CURRENT ROW AND CURRENT ROW) 
FROM ( 
         SELECT * from "t_alltype.parquet" 
     ) 
GROUP BY c1, c8
