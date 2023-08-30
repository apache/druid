-- with order by in over clause
explain plan for select sum(a1) over(order by c1) from t1;
