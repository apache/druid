select c1, c2, w_max from ( select c1, c2, max ( c1 ) over ( partition by c2 order by c1 asc nulls first ) w_max from "tblWnulls.parquet" ) sub_query where w_max is not null
