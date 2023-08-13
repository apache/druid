SELECT *
    FROM
    ( SELECT SUM(c2) OVER W as sm
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
WHERE subQry.sm > 0
