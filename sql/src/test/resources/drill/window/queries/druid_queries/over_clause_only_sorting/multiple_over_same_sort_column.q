select
countryName, cityName, added,
row_number() over (order by added) as c1,
lag(cityName) over (order by added) as c2
from wikipedia
where countryName in ('Egypt', 'El Salvador')
group by countryName, cityName, added
