SELECT
      *
FROM
    (
        SELECT c8, c1, 
            MAX(AVG(c1)) OVER W as mx_avg_c1 ,
            AVG(c2) OVER W as avg_c2 ,
            SUM(AVG(c3)) OVER W as sum_avg_c3 ,
            COUNT(AVG(c9)) OVER W as count_avg_c9
        FROM "t_alltype.parquet" GROUP BY c1,c2,c8
            WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
