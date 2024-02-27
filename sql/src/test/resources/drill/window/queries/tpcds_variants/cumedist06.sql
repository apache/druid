SELECT CUME_DIST() OVER (PARTITION BY s.ss_store_sk, s.ss_customer_sk ORDER BY s.ss_store_sk) FROM store_sales s LIMIT 20;
