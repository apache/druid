-- DRILL-3211 (requires group by)
select avg(a1), sum(a1) over(partition by b1) from t1 group by b1;
