-- left outer join between two tables
-- windowed aggregate on column from the left side
-- windowed aggregate on column from the right side
select
        j1.c_boolean,
        j1.c_date,
        sum(j1.c_integer)       over (partition by j1.c_boolean order by j1.c_date),
        avg(j4.c_integer)       over (partition by j4.c_boolean order by j4.c_date),
        rank()                  over (order by j1.c_integer)
from
        j1
                left outer join
        j4 on j1.c_integer = j4.c_integer
order by
        1, 2;
