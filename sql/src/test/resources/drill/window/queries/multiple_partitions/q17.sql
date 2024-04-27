-- Different window definitions
-- Aggregate and row_number window functions
-- One window is empty
select
        c_integer,
        c_date,
        c_time,
        row_number() over(order by c_date),
        avg(c_integer) over()
from
        j1
order by
        row_number() over(order by c_date);
