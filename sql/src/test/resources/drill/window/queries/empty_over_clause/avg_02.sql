select c_date, c_integer, avg(c_integer) over() from j7 order by 1, 2, 3;
