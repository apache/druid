select countryName, row_number() over () as c1
from wikipedia
where countryName in ('non-existent-country')
group by countryName, cityName, channel
