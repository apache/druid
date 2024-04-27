-- parition by only
select sum(a1) over(partition by c1) from t1;
