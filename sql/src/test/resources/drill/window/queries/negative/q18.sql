-- DRILL-3359
select sum(salary) over(partition by position_id order by salary rows between unbounded preceding   and 10 following) from cp.`employee.json`;
