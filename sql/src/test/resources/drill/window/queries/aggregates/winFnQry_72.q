select c1, dense_rank() over ( partition by c2 order by c1 asc nulls last ) dense_rnk from "tblWnulls.parquet"
