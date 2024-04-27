select c1, percent_rank() over ( partition by c2 order by c1 asc nulls last ) prct_rnk from "tblWnulls.parquet"
