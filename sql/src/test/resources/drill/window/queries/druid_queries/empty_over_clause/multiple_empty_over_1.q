select 
countryName, 
row_number() over () as c1, 
lag(countryName) over () as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
