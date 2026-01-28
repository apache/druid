SELECT
countryName,
cityName,
added,
count(added) OVER (PARTITION BY countryName, cityName),
sum(added) OVER (PARTITION BY countryName, cityName),
ROW_NUMBER() OVER (PARTITION BY countryName, cityName, added)
FROM "wikipedia"
where countryName in ('Guatemala', 'Austria')
