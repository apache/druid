-- Multiple window functions in with clause
with query as
(
select  sum(c_integer) over w,
        cast(avg(c_bigint) over w as double precision),
        count(*) over w
from    j7
window w as (partition by c_date, c_time order by c_date, c_time nulls first)
)
select * from query
order by 1,2,3;
