SELECT col6, col8 , LEAD(col8 ) OVER ( PARTITION BY col6 ORDER BY col6,col8 ) LEAD_col8 FROM "fewRowsAllData.parquet" WHERE col2 IN ('IN','SD','MO','SD','KS')
