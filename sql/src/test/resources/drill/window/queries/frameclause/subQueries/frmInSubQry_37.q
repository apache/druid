SELECT *
    FROM
    ( SELECT MAX(c2) OVER W as sm
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN CURRENT ROW AND CURRENT ROW )
    ) subQry
WHERE subQry.sm > 0
