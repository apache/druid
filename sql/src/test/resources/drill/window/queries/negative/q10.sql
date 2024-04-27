-- with the window clause
select  sum(distinct a1) over w,
        count(distinct a1) over w
from    t1
window w as (partition by b1 order by c1 desc);
