SELECT SUM(ss.ss_net_paid_inc_tax) OVER (PARTITION BY ss.ss_store_sk) AS TotalSpend FROM store_sales ss ORDER BY 1 LIMIT 20;
