-- join between two subqueries
-- one of the subqueries has a window function
select * from
(
select
        b1,
        sum(a1) over (partition by b1)
from    t1
) as sq1(b1, sum_a1)
        inner join
(
select  b2,
        sum(a2)
from    t2
group by
        b2
) as sq2(b2, sum_a2)
on (sq1.b1 = sq2.b2);
