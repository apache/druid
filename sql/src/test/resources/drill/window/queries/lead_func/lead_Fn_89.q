select * from (select lead(col3) over(partition by col7 order by col3) lead_col3 from "allTypsUniq.parquet") sub_query where lead_col3 is not null
