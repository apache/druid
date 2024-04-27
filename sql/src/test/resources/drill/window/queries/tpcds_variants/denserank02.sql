SELECT DENSE_RANK() OVER (ORDER BY ss.ss_store_sk) FROM store_sales ss LIMIT 20;
