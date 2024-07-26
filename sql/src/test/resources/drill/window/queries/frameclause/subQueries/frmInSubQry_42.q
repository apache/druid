SELECT *
    FROM
    ( SELECT MAX(c4) OVER W as w_max
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
WHERE subQry.w_max > 'k'
