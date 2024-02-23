select c1, count ( c1 ) over ( partition by c2 order by c1 asc nulls last ) from "tblWnulls.parquet"
