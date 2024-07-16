SELECT
countryName,
cityName,
ROW_NUMBER() OVER(PARTITION BY countryName),
channel,
COUNT(channel) over (PARTITION BY cityName order by countryName, cityName, channel)
FROM wikipedia
where countryName in ('Guatemala', 'Austria', 'Republic of Korea')
group by countryName, cityName, channel
