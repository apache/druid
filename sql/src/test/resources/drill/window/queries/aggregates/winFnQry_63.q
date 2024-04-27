select c1, c2, sum ( c1 ) over ( partition by c2 order by c1 desc nulls first ) w_sum from "tblWnulls.parquet"
