select cityName, countryName,
row_number() over (partition by countryName order by countryName, cityName, channel) as c1,
count(channel) over (partition by cityName order by countryName, cityName, channel) as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
