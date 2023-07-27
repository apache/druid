select  col7, col6 , col0 , lead(col0) over(partition by col7 order by col6) lead_col0 from "allTypsUniq.parquet"
