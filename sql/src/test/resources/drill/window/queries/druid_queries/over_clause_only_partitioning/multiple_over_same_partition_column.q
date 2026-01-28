select
countryName, cityName, channel,
row_number() over (PARTITION BY channel) as c1,
lag(cityName) over (PARTITION BY channel) as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
