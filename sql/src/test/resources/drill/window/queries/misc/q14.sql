-- aggregation on top of window function
select distinct *
from
(
select  sum(c_integer) over w,
        cast(avg(c_bigint) over w as double precision),
        count(*) over w
from    j7
window w as (partition by c_date order by c_time desc)
) as foo;
