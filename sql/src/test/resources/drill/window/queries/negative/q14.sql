-- DRILL-3359
select sum(salary) over(partition by position_id order by salary rows between unbounded preceding and current row) from cp.`employee.json`;

