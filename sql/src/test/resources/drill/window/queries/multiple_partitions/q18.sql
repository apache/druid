-- 3 windows with different default defintions
-- all windows have only order by clause
-- mix of aggregate and ranking functions

select
	max(c_bigint) over(order by c_varchar, c_date) as max_value,
	min(c_bigint) over(order by c_date, c_varchar) as min_value,
	row_number()  over(order by c_integer + 1) as row_num	

from
	j4
order by
	row_number() over(order by c_integer + 1) desc nulls first;
