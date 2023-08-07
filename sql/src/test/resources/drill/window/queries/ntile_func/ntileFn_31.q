SELECT * FROM (SELECT col7 , col0 , NTILE(2) OVER (PARTITION by col7 ORDER by col0) tile FROM "allTypsUniq.parquet") sub_query WHERE tile = 2
