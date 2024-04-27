select c1, max ( c1 ) over ( partition by c2 order by c1 asc nulls last ) w_max, c2 from "tblWnulls.parquet"
