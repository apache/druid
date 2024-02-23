-- join between two subqueries
-- one of the subqueries has a window function
-- result of window function is a join column

select * from
(       
select  
        row_number() over (order by a1),
        sum(a1) over (partition by b1)
from    t1
) as sq1(b1, sum_a1)
        inner join 
(
select  a2,
        sum(a2) over(partition by b2, c2 order by a2)
from    t2
) as sq2(b2, sum_a2)
on (sq1.b1 = sq2.b2);
