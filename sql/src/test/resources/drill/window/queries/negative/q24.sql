-- DRILL-3211 (requires group by)
select sum(a2), sum(a2) over(partition by b2) from t2 group by b2;
