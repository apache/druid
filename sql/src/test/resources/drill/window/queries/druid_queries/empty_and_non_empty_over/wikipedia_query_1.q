select
countryName, cityName, channel, added,
row_number() over () as c1,
row_number() over (PARTITION BY countryName, cityName) as c2,
row_number() over (PARTITION BY cityName, channel ORDER BY channel) as c3
from wikipedia
where countryName in ('Guatemala', 'Austria')
group by countryName, cityName, channel, added
