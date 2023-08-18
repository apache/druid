select c_integer as a, percent_rank() over(order by c_integer) as b from j1 order by  a, b desc;
