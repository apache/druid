select c_integer, avg(c_integer) over(partition by c_bigint order by c_date desc range unbounded preceding) from j9 order by 1, 2;
