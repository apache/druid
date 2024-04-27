-- NPE when two different window functions are used in projection list and order by clauses
select
        a1,
        rank() over(partition by b1 order by a1)
from
        t1
order by
        row_number() over(partition by b1 order by a1);

