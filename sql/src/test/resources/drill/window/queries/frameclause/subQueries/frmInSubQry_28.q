SELECT *
    FROM
    ( SELECT MIN(c7) OVER W as w_min
      FROM "t_alltype.parquet"
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN CURRENT ROW AND CURRENT ROW )
    ) subQry
WHERE subQry.w_min = TIMESTAMP_TO_MILLIS(TIME_PARSE('1912-02-01', 'yyyy-MM-dd'))
