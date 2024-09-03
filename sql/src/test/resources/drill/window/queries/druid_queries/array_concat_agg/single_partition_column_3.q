select
countryName, added, array_concat_agg(ARRAY[added], 10000) over (partition by added) as c1
from wikipedia where countryName in ('Guatemala', 'Austria')
group by countryName, added
