CREATE OR REPLACE VIEW vwOnParq_110 (col_int, col_bigint, col_char_2, col_vchar_52, col_tmstmp, col_dt, col_booln, col_dbl, col_tm) AS SELECT col_int, col_bigint, col_char_2, col_vchar_52, col_tmstmp, col_dt, col_booln, col_dbl, col_tm from "forViewCrn.parquet"
SELECT COUNT(MIN(col_dbl)) OVER(PARTITION BY col_char_2 ORDER BY col_int) min_dbl, col_char_2 FROM vwOnParq_110 GROUP BY col_char_2,col_int
DROP VIEW vwOnParq_110
