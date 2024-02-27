-- start query 53 in stream 0 using template query53.tpl 
SELECT * 
FROM   (
SELECT manufact_id,
               sum_sales,
               Avg(sum_sales)
                 OVER (
                   partition BY manufact_id) avg_quarterly_sales
FROM (
SELECT i.i_manufact_id manufact_id, 
               Sum(ss.ss_sales_price)             sum_sales 
        FROM   item i, 
               store_sales ss, 
               date_dim d, 
               store s 
        WHERE  ss.ss_item_sk = i.i_item_sk 
               AND ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND d.d_month_seq IN ( 1199, 1199 + 1, 1199 + 2, 1199 + 3, 
                                    1199 + 4, 1199 + 5, 1199 + 6, 1199 + 7, 
                                    1199 + 8, 1199 + 9, 1199 + 10, 1199 + 11 ) 
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
        GROUP  BY i.i_manufact_id, 
                  d.d_qoy) tmp1
) 
WHERE  CASE 
         WHEN avg_quarterly_sales > 0 THEN Abs (sum_sales - avg_quarterly_sales) 
                                           / 
                                           avg_quarterly_sales 
         ELSE NULL 
       END > 0.1 
ORDER  BY avg_quarterly_sales, 
          sum_sales, 
          manufact_id
LIMIT 100; 
