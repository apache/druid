select
countryName, cityName,
lag(cityName) over (PARTITION BY cityName) as c1
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
