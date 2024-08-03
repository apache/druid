select
countryName, cityName, channel,
lag(cityName) over (order by channel, countryName) as c1
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
