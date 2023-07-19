-- partition by only
-- with expression
select sum(a1 + 0) over(partition by c1) from t1;
