-- DRILL-3211 (requires group by)
select sum(a2), row_number() over(partition by c2, b2 order by a2) from t2 group by c2,b2;
