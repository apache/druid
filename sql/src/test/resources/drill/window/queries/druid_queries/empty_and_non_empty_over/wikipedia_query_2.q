select
countryName, cityName, channel,
row_number() over () as c1,
lag(cityName) over (PARTITION BY countryName, cityName) as c2,
lead(cityName) over (PARTITION BY cityName, added) as c3
from wikipedia
where countryName in ('Guatemala', 'Austria')
group by countryName, cityName, channel, added
