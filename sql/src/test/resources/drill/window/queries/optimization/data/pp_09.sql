-- with both partition and order by
select sum(a1) over(partition by c1 order by c1) from t1;
