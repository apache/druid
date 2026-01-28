select countryName, cityName, channel, array_concat_agg(ARRAY['N/A', countryName], 10000) over (order by countryName) as c
from wikipedia
where countryName in ('Austria', 'Republic of Korea') and (cityName in ('Horsching', 'Vienna', 'Seoul', 'Jeonju') or cityName is null)
group by countryName, cityName, channel
