SELECT
AVG(added) OVER(PARTITION BY countryName),
countryName
FROM wikipedia
where countryName in ('Guatemala', 'Austria')
