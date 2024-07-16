select cityName, countryName,
row_number() over w1 as c1,
count(channel) over w2 as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
WINDOW
	w1 AS (partition by countryName order by countryName, cityName, channel),
	w2 AS (partition by cityName order by countryName, cityName, channel)
