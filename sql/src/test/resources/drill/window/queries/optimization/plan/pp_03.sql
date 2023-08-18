-- parition by only
explain plan for select sum(a1) over(partition by c1) from t1;
