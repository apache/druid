-- join + aggregation
-- different window functions with different window clauses
explain plan for
select
        sum(t1.a1) over (partition by t2.b2),
        avg(t2.a2) over (partition by t1.b1)
from
        t1, t2
where
        t1.c1 = t2.c2
group by
        t1.a1,
        t1.b1,
        t2.a2,
        t2.b2;
