select c1, c2, avg ( c1 ) over ( partition by c2 order by c1 asc nulls first ) w_avg from "tblWnulls.parquet"
