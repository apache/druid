select
countryName, cityName, channel,
row_number() over (partition by array[1,2,3] order by countryName) as c
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
