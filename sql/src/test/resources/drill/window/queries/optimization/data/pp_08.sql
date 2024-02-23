-- with order by in over clause
select sum(a1) over(order by c1) from t1;
