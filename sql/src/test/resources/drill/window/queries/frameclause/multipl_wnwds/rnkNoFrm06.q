SELECT 
      c8, c1, c2,
      RANK() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_rnk,
      SUM(c2) OVER w as w_sum_c2, 
      DENSE_RANK() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_dnsrnk,
      MIN(c2) OVER w as w_min_c2,
      ROW_NUMBER() OVER ( PARTITION BY c8 ORDER BY c1 ) as w_rwnum,
      MAX(c2) OVER w as w_max_c2, 
      FIRST_VALUE(c2) OVER ( PARTITION BY c8 ORDER BY c1 ) as w_fval_c2,
      AVG(c2) OVER w as w_avg_c2, 
      COUNT(c2) OVER w as w_count_c2,
      LAST_VALUE(c2) OVER ( PARTITION BY c8 ORDER BY c1 ) as w_lval_c2
FROM
     "t_alltype.parquet" WINDOW w AS (PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
