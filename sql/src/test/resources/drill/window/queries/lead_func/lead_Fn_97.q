select col6, lead(col6) over(partition by col7 order by col6) lead_col6
from "allTypsUniq.parquet"
where
    col6 > TIMESTAMP_TO_MILLIS(TIME_PARSE('1947-05-12', 'yyyy-MM-dd')) and
    col6 < TIMESTAMP_TO_MILLIS(TIME_PARSE('2007-10-01', 'yyyy-MM-dd'))