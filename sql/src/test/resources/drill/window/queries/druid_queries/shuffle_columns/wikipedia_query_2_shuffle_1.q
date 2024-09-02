SELECT
ROW_NUMBER() OVER(PARTITION BY countryName order by countryName, cityName, channel),
countryName,
cityName,
COUNT(channel) over (PARTITION BY cityName order by countryName, cityName, channel),
channel
FROM wikipedia
where countryName in ('Guatemala', 'Austria', 'Republic of Korea')
group by countryName, cityName, channel
