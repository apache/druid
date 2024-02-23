SELECT MAX(cast( columns[8] as TIME )) OVER(PARTITION BY cast( columns[2] as CHAR(2)) ORDER BY cast( columns[0] as INT )), columns[2] FROM "allData.csv" limit 20
