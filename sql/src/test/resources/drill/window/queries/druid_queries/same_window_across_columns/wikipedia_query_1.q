SELECT countryName,
sum("deleted")  OVER (PARTITION BY countryName) as count_c3,
sum(delta)  OVER (PARTITION BY countryName) as count_c1,
sum(added)  OVER (PARTITION BY countryName) as count_c2
FROM "wikipedia"
where countryName in ('Guatemala', 'Austria')
