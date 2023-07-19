SELECT
      *
FROM
    (
        SELECT
            COUNT(c1) OVER W as count_c1 ,
            COUNT(c2) OVER W as count_c2 ,
            COUNT(c3) OVER W as count_c3 ,
            COUNT(c4) OVER W as count_c4 ,
            COUNT(c5) OVER W as count_c5 ,
            COUNT(c6) OVER W as count_c6 ,
            COUNT(c7) OVER W as count_c7 ,
            COUNT(c8) OVER W as count_c8 ,
            COUNT(c9) OVER W as count_c9
        FROM "t_alltype.parquet"
            WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
