SELECT
      *
FROM
    (
        SELECT
            AVG(c1) OVER W as avg_c1 ,
            AVG(c2) OVER W as avg_c2 ,
            AVG(c3) OVER W as avg_c3 ,
            AVG(c9) OVER W as avg_c9
        FROM "t_alltype.parquet"
            WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ) subQry
