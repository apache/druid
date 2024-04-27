-- two different window clauses with GROUP BY
-- aggregate function, windowed aggregate, row_number()
-- only order by clause in window definition
select
	c_integer,
        sum(c_integer),
        row_number()   over (order by c_date desc),
        ntile(100) over (order by c_date, c_timestamp, c_time )
from
        j1
group by
        c_integer,
        c_date,
	c_timestamp,
	c_time
order by
        row_number()   over (order by c_date)
;

