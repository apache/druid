select c1, cume_dist() over ( partition by c2 order by c1 asc nulls last ) cume_dst from "tblWnulls.parquet"
