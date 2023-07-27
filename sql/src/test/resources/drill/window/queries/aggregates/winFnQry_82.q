select c1, c2, w_avg from ( select c1, c2, avg ( c1 ) over ( partition by c2 order by c1 asc nulls first ) w_avg from "tblWnulls.parquet" ) sub_query where w_avg is not null
