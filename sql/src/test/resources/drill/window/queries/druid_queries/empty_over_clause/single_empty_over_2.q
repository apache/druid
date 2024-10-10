select countryName, lag(countryName) over () as c1
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
