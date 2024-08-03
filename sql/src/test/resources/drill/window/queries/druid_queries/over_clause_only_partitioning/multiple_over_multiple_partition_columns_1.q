select
countryName, cityName,
row_number() over (PARTITION BY countryName, cityName) as c1,
lag(cityName) over (PARTITION BY cityName, countryName) as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName
