-- functions in window function
select avg(coalesce(a1, 100)) over (partition by c1 order by b1) from t1;
