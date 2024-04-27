SELECT *
    FROM
    ( SELECT MAX(c4) OVER W as w_max
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ) subQry
WHERE subQry.w_max = 'yytBlhCMRSmV'
