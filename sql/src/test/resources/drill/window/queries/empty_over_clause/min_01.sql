select c_date, c_integer, min(c_integer) over() from j1 order by 1, 2, 3;
