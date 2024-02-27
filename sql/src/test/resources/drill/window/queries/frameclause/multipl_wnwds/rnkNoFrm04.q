SELECT 
      RANK() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_rnk,
      DENSE_RANK() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_dnsRnk,
      ROW_NUMBER() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_rwnum,
      SUM(c2) OVER w as w_sum, 
      MIN(c2) OVER w as w_min,
      MAX(c2) OVER w as w_max, 
      AVG(c2) OVER w as w_avg, 
      COUNT(c2) OVER w as w_count,
      FIRST_VALUE(c2) OVER ( PARTITION BY c8 ORDER BY c1 ) as w_fval,
      LAST_VALUE(c2) OVER ( PARTITION BY c8 ORDER BY c1 ) as w_lval
FROM
     "t_alltype.parquet" WINDOW w AS (PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
