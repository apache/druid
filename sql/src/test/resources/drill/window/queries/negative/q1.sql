select
        avg(a1) over (partition by b1 order by c1 range between unbounded preceding and unbounded following ),
        sum(a1) over (partition by b1 order by c1 range between unbounded preceding and unbounded following ),
        row_number() over (partition by b1 order by c1 range between unbounded preceding and unbounded following )
from    t1;
