-- join between two subqueries
-- both of the subqueries have window functions (different windows)
-- WF result is NOT a join column

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
        sum(a2) over(partition by b2, c2 order by a2)
from    t2
group by
        a2, b2, c2
) as sq2(b2, sum_a2)
on (sq1.b1 = sq2.b2);
