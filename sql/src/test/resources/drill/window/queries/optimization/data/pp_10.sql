-- functions in window function
select avg(cast(a1 as bigint)) over (partition by c1 order by b1) from t1;
