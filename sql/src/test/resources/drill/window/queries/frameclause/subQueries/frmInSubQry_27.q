SELECT *
    FROM
    ( SELECT c8,c1,c7,MIN(c7) OVER W as w_min
      FROM "t_alltype.parquet"
--where c7 is not null
          WINDOW W AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
    ) subQry
WHERE MILLIS_TO_TIMESTAMP(subQry.w_min) > '1920-05-14'
--and subQry.w_min is not null
