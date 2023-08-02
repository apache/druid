-- start query 63 in stream 0 using template query63.tpl 
SELECT * 
FROM   (
SELECT         manager_id,
               sum_sales,
               Avg(sum_sales)
                 OVER (
                   partition BY manager_id) avg_monthly_sales
FROM 
(
SELECT         i.i_manager_id manager_id, 
               Sum(ss.ss_sales_price)            sum_sales 
        FROM   item i, 
               store_sales ss, 
               date_dim d,
               store s 
        WHERE  ss.ss_item_sk = i.i_item_sk 
               AND ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND d.d_month_seq IN ( 1200, 1200 + 1, 1200 + 2, 1200 + 3, 
                                    1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7, 
                                    1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11 ) 
               AND ( ( i.i_category IN ( 'Books', 'Children', 'Electronics' ) 
                       AND i.i_class IN ( 'personal', 'portable', 'reference', 
                                        'self-help' ) 
                       AND i.i_brand IN ( 'scholaramalgamalg #14', 
                                        'scholaramalgamalg #7' 
                                        , 
                                        'exportiunivamalg #9', 
                                                       'scholaramalgamalg #9' ) 
                     ) 
                      OR ( i.i_category IN ( 'Women', 'Music', 'Men' ) 
                           AND i.i_class IN ( 'accessories', 'classical', 
                                            'fragrances', 
                                            'pants' ) 
                           AND i.i_brand IN ( 'amalgimporto #1', 
                                            'edu packscholar #1', 
                                            'exportiimporto #1', 
                                                'importoamalg #1' ) ) ) 
        GROUP  BY i.i_manager_id, 
                  d.d_moy
)) tmp1 
WHERE  CASE 
         WHEN avg_monthly_sales > 0 THEN Abs (sum_sales - avg_monthly_sales) / 
                                         avg_monthly_sales 
         ELSE NULL 
       END > 0.1 
ORDER  BY manager_id, 
          avg_monthly_sales, 
          sum_sales
LIMIT 100; 
