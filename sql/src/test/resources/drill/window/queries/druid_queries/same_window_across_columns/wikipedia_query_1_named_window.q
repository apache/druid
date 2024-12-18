SELECT countryName,
sum("deleted")  OVER w as count_c3,
sum(delta)  OVER w as count_c1,
sum(added)  OVER w as count_c2
FROM "wikipedia"
where countryName in ('Guatemala', 'Austria')
WINDOW w AS (PARTITION BY countryName)
