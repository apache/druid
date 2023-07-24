-- case in window function
select avg(case when a1 is not null then a1/100 else 100 end) over (partition by c1) from t1;
