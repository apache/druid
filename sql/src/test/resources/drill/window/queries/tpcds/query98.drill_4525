SELECT i_item_id, 
       i_item_desc, 
       i_category, 
       i_class, 
       i_current_price, 
       Sum(ss_ext_sales_price)                                   AS itemrevenue, 
       Sum(ss_ext_sales_price) * 100 / Sum(Sum(ss_ext_sales_price)) 
                                         OVER ( 
                                           PARTITION BY i_class) AS revenueratio 
FROM   store_sales, 
       item, 
       date_dim 
WHERE  ss_item_sk = i_item_sk 
       AND i_category IN ( 'Men', 'Home', 'Electronics' ) 
       AND ss_sold_date_sk = d_date_sk 
       AND d_date BETWEEN CAST('2000-05-18' AS DATE) AND ( 
                          CAST('2000-05-18' AS DATE) + INTERVAL '30' DAY ) 
GROUP  BY i_item_id, 
          i_item_desc, 
          i_category, 
          i_class, 
          i_current_price 
ORDER  BY i_category, 
          i_class, 
          i_item_id, 
          i_item_desc, 
          revenueratio; 
