select
countryName, cityName, added,
sum(added),
count(added) over () e1,
ROW_NUMBER() over () e2,
ROW_NUMBER() over (order by added) e3,
lag(added) over (order by cityName, countryName) e4,
count(*) over (partition by cityName order by countryName rows between unbounded preceding and current row) c2,
count(*) over (partition by cityName order by countryName rows between unbounded preceding and 1 following) c3,
count(*) over (partition by cityName order by countryName rows between unbounded preceding and 1 preceding) c4,
count(*) over (partition by cityName order by countryName rows between 3 preceding and 1 preceding) c5,
count(*) over (partition by cityName order by countryName rows between 1 preceding and current row) c5,
count(*) over (partition by cityName order by countryName rows between 1 preceding and 1 FOLLOWING) c7,
count(*) over (partition by cityName order by countryName rows between 1 preceding and unbounded FOLLOWING) c5,
lag(countryName) over (order by added) e5,
count(*) over (partition by cityName order by countryName rows between 1 FOLLOWING and unbounded FOLLOWING) c5,
count(*) over (partition by cityName order by countryName rows between 1 FOLLOWING and 3 FOLLOWING) c10,
ROW_NUMBER() over (partition by cityName order by added) e6,
count(*) over (partition by cityName order by countryName rows between current row and 1 following) c11,
count(*) over (partition by cityName order by countryName rows between current row and unbounded following) c12
from wikipedia
where cityName in ('Vienna', 'Seoul')
group by countryName, cityName, added
