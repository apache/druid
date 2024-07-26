select col5, lead(col5) over(partition by col7 order by col5) lead_col5
from "allTypsUniq.parquet"
where
    col5 >= TIMESTAMP_TO_MILLIS(TIME_PARSE('1947-07-02 00:28:02.418', 'yyyy-MM-dd HH:mm:ss.SSS')) and
    col5 < TIMESTAMP_TO_MILLIS(TIME_PARSE('2011-06-02 00:28:02.218', 'yyyy-MM-dd HH:mm:ss.SSS'))