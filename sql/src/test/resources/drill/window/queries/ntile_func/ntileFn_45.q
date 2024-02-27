select tile, count(tile) from (select c1, c2, ntile(3) over(partition by c2 order by c1) tile from "tblWnulls.parquet") sub_query where c1 is not null group by tile
