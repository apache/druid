-- join between two subqueries
-- both subqueries have multiple window functions with different window clauses
-- result of window function is not a join column
select * from
(
select
        b1,
        sum(a1) over (partition by b1, c1),
	dense_rank() over (partition by b1 order by b1)
from    t1
) as sq1(b1, sum_a1, dense_rank_col)
        inner join
(
select  b2,
        sum(a2)  	over (partition by b2),
	avg(a2)  	over (partition by b2),
	row_number()  	over (order by a2) 	
from    t2
) as sq2(b2, sum_a2, avg_a2, row_number_col)
on (sq1.b1 = sq2.b2);
