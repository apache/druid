-- with both partition and order by
explain plan for select sum(a1) over(partition by c1 order by c1) from t1;
