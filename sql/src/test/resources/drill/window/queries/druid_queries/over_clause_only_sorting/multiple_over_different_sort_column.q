select
countryName, cityName, page, delta,
row_number() over (order by page) as c1,
lag(cityName) over (order by delta) as c2
from wikipedia
where countryName in ('Guatemala', 'El Salvador')
group by countryName, cityName, page, delta
