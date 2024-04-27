-- DRILL-3359
select sum(salary) over(partition by position_id order by salary rows between unbounded preceding   and unbounded following) from cp.`employee.json`;
