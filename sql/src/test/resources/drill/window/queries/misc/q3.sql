select j1.c_boolean, j1.c_date, sum(j1.c_integer) over (partition by j1.c_boolean order by j4.c_date) from j1 left outer join j4 on j1.c_integer = j4.c_integer order by 1, 2;
