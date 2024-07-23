SELECT
countryName,
cityName,
added,
count(added) OVER (PARTITION BY countryName, cityName)
FROM "wikipedia"
where countryName in ('Guatemala', 'Austria')
