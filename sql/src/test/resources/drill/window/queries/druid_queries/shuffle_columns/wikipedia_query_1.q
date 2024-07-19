SELECT
countryName,
AVG(added) OVER(PARTITION BY countryName)
FROM wikipedia
where countryName in ('Guatemala', 'Austria')
