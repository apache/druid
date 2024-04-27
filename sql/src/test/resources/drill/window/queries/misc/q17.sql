--q17.sql
-- multiple aggregate functions in the same query
-- all on the same column from one table
-- over clause is empty
select
        j6.c_integer,
        sum(j6.c_integer) over w,
        max(j6.c_integer) over w,
        min(j6.c_integer) over w,
        count(*) over w
from j6
window  w as ()
order by
        1, 2, 3, 4, 5;
