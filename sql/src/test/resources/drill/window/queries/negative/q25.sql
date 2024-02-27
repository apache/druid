-- DRILL-3211 (requires group by)
select sum(a2), rank() over(partition by b2 order by a2) from t2 group by b2;
