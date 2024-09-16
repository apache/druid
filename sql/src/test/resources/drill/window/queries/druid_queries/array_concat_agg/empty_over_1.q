select
countryName, array_concat_agg(ARRAY[countryName], 10000) over () as c1
from wikipedia where countryName='Guatemala'
group by countryName
