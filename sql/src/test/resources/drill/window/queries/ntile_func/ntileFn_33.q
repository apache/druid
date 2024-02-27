SELECT col0 FROM "allTypsUniq.parquet" ORDER by NTILE(2) OVER (PARTITION by col7 ORDER by col0), col1
