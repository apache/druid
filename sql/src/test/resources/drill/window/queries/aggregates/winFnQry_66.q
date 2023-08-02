select c1, min ( c1 ) over ( partition by c2 order by c1 asc nulls last ) w_min, c2 from "tblWnulls.parquet"
