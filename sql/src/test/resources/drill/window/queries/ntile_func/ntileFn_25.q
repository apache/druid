SELECT col7 , NTILE(4) OVER (PARTITION by col7 ORDER by col4) tile FROM "allTypsUniq.parquet"
