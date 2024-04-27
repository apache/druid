-- Window functions in the case statement in a subquery
-- Result of Windowed function is an input to MIN/MAX window functions
SELECT 
	c_date, 
	c_timestamp, 
	c_time, 
	col1, 
	col2, 
	MIN(col1) OVER (ORDER BY c_date), 
	MAX(col2) OVER () 
FROM
	(
        select 	*,
                case when c_date < '2008-01-01' then 2008 - extract(year from c_date) end * 500 as col1,
                case when
                        avg(c_integer) over (partition by c_varchar) > c_bigint
                then 200 end 									as col2 
	FROM 
		j6
)s;

