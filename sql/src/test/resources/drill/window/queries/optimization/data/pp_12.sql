-- window function in case statement
select case when c1 is not null then first_value(a1) over (partition by c1 order by b1) else 100 end from t1;
