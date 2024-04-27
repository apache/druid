-- partition by only
-- with expression
explain plan for select sum(a1 + 0) over(partition by c1) from t1;
