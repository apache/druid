-- Drill-3574 - empty over clause should trigger union-exchange
alter session set `planner.slice_target` = 1;

select
        a1,
        sum(a1) over(partition by b1, c1),
        sum(a1) over()
from
        t1
order by
        a1;

alter session set `planner.slice_target` = 100000;
