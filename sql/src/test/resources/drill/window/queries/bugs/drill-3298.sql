alter session set `planner.slice_target` = 1;

select
        j1.c_integer,
        sum(j1.c_integer) over w
from j1
window  w as (order by c_integer desc)
order by
        1, 2;

alter session set `planner.slice_target` = 100000;
