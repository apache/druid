select * from (select lead(col1) over(partition by col7 order by col1) lead_col1 from "allTypsUniq.parquet") sub_query where lead_col1 is not null
