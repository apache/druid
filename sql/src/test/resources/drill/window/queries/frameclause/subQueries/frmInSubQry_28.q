SELECT *
    FROM
    ( SELECT MIN(c7) OVER W as w_min
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN CURRENT ROW AND CURRENT ROW )
    ) subQry
WHERE subQry.w_min = '1912-02-01'
