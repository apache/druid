select
        j2.c_boolean,
        j2.c_date,
        j2.c_integer,
        sum(j2.c_integer) over (partition by j2.c_boolean order by j2.c_date, j2.c_integer)
from    j1
           left outer join
        j2 on j1.c_integer = j2.c_integer
order by 1,2,3;
