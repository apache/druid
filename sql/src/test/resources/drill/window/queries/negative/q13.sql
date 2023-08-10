-- DRILL-3359
select sum(salary) over(partition by position_id order by salary rows unbounded preceding) from cp.`employee.json`;
