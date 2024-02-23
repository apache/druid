select
        lag(c_integer) over(order by c_integer nulls first),
        lag(c_bigint) over(partition by c_time order by c_date),
        lag(c_integer) over (partition by c_time order by c_date),
        lag(c_bigint) over(partition by c_time order by c_date),
        lag(c_integer) over (partition by c_time order by c_date)
from
        j4;

