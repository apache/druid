SELECT col4 , LAG(col4) OVER (ORDER BY col4) LAG_col4 FROM "fewRowsAllData.parquet" WHERE col4 < '2012-06-02 00:28:02.433'
