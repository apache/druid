SELECT col7 , col4, NTILE(3) OVER (PARTITION by col7 ORDER by col4) tile FROM "allTypsUniq.parquet"
