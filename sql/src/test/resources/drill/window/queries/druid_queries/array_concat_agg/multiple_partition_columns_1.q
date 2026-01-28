select
countryName, cityName, added,
array_concat_agg(ARRAY[added], 10000) over (partition by countryName, cityName) as c1
from wikipedia where countryName in ('Guatemala', 'Austria')
group by countryName, cityName, added
