SELECT col7, LEAD(col7 ) OVER ( PARTITION BY col2 ORDER BY col2,col7 nulls FIRST ) LEAD_col7, col2 FROM "fewRowsAllData.parquet" WHERE col2 IN ('VT','AZ','CO','GA','IN','MN','RI')
