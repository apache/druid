select c_integer as a, row_number() over(order by c_integer) as b from j1 order by  a, b desc;
