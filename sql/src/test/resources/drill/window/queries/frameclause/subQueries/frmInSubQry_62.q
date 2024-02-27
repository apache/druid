SELECT
      *
FROM
    (
        SELECT
            LAST_VALUE(c1) OVER W as lastVal_c1 ,
            LAST_VALUE(c2) OVER W as lastVal_c2 ,
            LAST_VALUE(c3) OVER W as lastVal_c3 ,
            LAST_VALUE(c4) OVER W as lastVal_c4 ,
            LAST_VALUE(c5) OVER W as lastVal_c5 ,
            LAST_VALUE(c6) OVER W as lastVal_c6 ,
            LAST_VALUE(c7) OVER W as lastVal_c7 ,
            LAST_VALUE(c8) OVER W as lastVal_c8 ,
            LAST_VALUE(c9) OVER W as lastVal_c9
        FROM "t_alltype.parquet"
            WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ) subQry
