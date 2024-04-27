select
        lead(c_integer) over(order by c_integer nulls first),
        lead(c_bigint) over(partition by c_time order by c_time),
        lead(c_integer) over (partition by c_time order by c_time),
        lead(c_bigint) over(partition by c_time order by c_date),
        lead(c_integer) over (partition by c_time order by c_date)
from
        j4;
