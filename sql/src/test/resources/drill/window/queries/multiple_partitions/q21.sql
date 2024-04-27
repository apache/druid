-- Multiple window defintions
select
        sum(c_integer) over w1,
        count(*)       over w2
from
        j1
window w1 as (order by c_date), w2 as (partition by c_bigint order by c_date, c_time)
;
