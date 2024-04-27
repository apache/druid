-- q15.sql
-- multiple aggregate functions in the same query
-- all on the same column from one table

select
        j1.c_integer,
        sum(j1.c_integer) over w,
        --cast(avg(j1.c_integer) over w as double precision),
        max(j1.c_integer) over w,
        min(j1.c_integer) over w,
        count(*) over w,
        rank() over w,
        dense_rank() over w,
        percent_rank() over w,
        cume_dist() over w,
        row_number() over w
from j1
window  w as (order by c_integer desc)
order by
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

