-- Complex expression with string manipulation functions
select
	cast(row_number() over(order by c_integer) as varchar(5))
	||
        trim(max(upper(c_varchar) || upper(c_varchar)) over() )
	|| 
	cast(min(c_integer) over() as varchar(10))
	||
        trim(max(concat(upper(c_varchar) , upper(c_varchar))) over(partition by c_date))
	||
        min(upper(c_varchar) || lower(c_varchar)) over(partition by c_boolean, c_date order by 1)
	||
        max(upper(c_varchar) || upper(c_varchar)) over(partition by c_date, c_date order by 1)
from
        j1;
