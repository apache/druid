SELECT 
      RANK() OVER w as w_rnk,
      DENSE_RANK() OVER w as w_dnsRnk,
      ROW_NUMBER() OVER w as w_rwnum,
      SUM(c2) OVER w as w_sum, 
      MIN(c2) OVER w as w_min,
      MAX(c2) OVER w as w_max, 
      AVG(c2) OVER w as w_avg, 
      COUNT(c2) OVER w as w_count,
      FIRST_VALUE(c2) OVER (PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as w_fval,
      LAST_VALUE(c2) OVER (PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as w_lval
FROM
     "t_alltype.parquet" WINDOW w AS (PARTITION BY c8 ORDER BY c1)
