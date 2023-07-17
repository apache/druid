SELECT
      *
FROM
    (
        SELECT
            FIRST_VALUE(c1) OVER W as firstVal_c1 ,
            FIRST_VALUE(c2) OVER W as firstVal_c2 ,
            FIRST_VALUE(c3) OVER W as firstVal_c3 ,
            FIRST_VALUE(c4) OVER W as firstVal_c4 ,
            FIRST_VALUE(c5) OVER W as firstVal_c5 ,
            FIRST_VALUE(c6) OVER W as firstVal_c6 ,
            FIRST_VALUE(c7) OVER W as firstVal_c7 ,
            FIRST_VALUE(c8) OVER W as firstVal_c8 ,
            FIRST_VALUE(c9) OVER W as firstVal_c9
        FROM "t_alltype.parquet"
            WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ) subQry
