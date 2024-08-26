select
countryName, added,
row_number() over (order by added) as c1
from wikipedia
where countryName in ('Egypt', 'El Salvador')
group by countryName, cityName, added
