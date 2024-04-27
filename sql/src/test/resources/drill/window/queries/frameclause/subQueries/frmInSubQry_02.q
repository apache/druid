SELECT *
    FROM
    ( SELECT SUM(c1) OVER W as w_sum
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
WHERE subQry.w_sum > 0
