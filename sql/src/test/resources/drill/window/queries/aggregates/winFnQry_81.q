select c1, c2, w_sum from ( select c1, c2, sum ( c1 ) over ( partition by c2 order by c1 asc nulls first ) w_sum from "tblWnulls.parquet" ) sub_query where w_sum is not null
