-- Multiple window definitions
-- where parameter to aggregate window function is CASE expression
select 
        avg(CASE WHEN c_integer is null THEN 100 ELSE c_integer END) over()     as avg_col,
        sum(coalesce(c_integer, 100)) over(partition by c_date)         	as sum_col,
        count(*)  over(partition by c_boolean)                          	as count_col
from 
        j1
order by
        1,2,3;   
