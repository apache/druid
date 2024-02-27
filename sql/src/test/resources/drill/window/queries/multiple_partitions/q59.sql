-- left outer join between two subqueries
-- both of the subqueries have window functions
-- join columns are result of window function computation

select * from
(
select
        row_number() over (order by a1),
        sum(a1) over (partition by b1)
from    t1
) as sq1(b1, sum_a1)
        left outer join
(
select  dense_rank() over(order by a2),
        sum(a2) over(partition by b2, c2 order by a2)
from    t2
) as sq2(b2, sum_a2)
on (sq1.b1 = sq2.b2);

