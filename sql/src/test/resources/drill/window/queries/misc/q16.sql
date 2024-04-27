--q16.sql
-- multiple window aggregate functions in the same query
-- all on the same column from one table
-- over clause has only "partition by clause" on a different column
select
        j7.c_integer,
        sum(j7.c_integer) over w,
        max(j7.c_integer) over w,
        min(j7.c_integer) over w,
        count(*) over w
from j7
window  w as (partition by c_date, c_bigint, c_boolean)
order by
        1, 2, 3, 4, 5;
