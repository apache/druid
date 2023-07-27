SELECT DENSE_RANK() OVER (PARTITION BY s.ss_store_sk, s.ss_customer_sk ORDER BY s.ss_store_sk, s.ss_customer_sk) FROM store_sales s LIMIT 20;
