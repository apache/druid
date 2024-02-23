select c1, sum ( c1 ) over ( partition by c2 order by c1 asc nulls last ) w_sum, c2 from "tblWnulls.parquet"
