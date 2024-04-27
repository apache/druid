select c1, avg ( c1 ) over ( partition by c2 order by c1 asc nulls last ) w_avg, c2 from "tblWnulls.parquet"
