select
countryName, cityName, channel, added, page, delta,
row_number() over (order by countryName, cityName, page) as c1,
lag(cityName) over (order by channel, added, delta) as c2
from wikipedia
where countryName in ('Guatemala', 'El Salvador')
group by countryName, cityName, channel, added, page, delta
