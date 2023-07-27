select col4 , lead(col4) over(partition by col7 order by col4,col7) lead_col4 from "allTypsUniq.parquet"
