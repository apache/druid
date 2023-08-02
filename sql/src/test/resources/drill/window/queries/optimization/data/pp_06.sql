-- join
-- window functions used in expression
select avg(t1.a1) over(partition by t1.c1) + avg(t2.a2) over(partition by t1.c1) from t1, t2 where t1.b1 = t2.b2;
