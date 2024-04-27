select col4, lead(col4) over(partition by col7 order by col4) lead_col4
from "allTypsUniq.parquet"
where col4 not in (
      TIMESTAMP_TO_MILLIS(TIME_PARSE('20:20:20.300', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('19:24:45.200', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('23:45:35.120', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('23:23:30.222', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('16:35:45.100', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('10:59:58.119', 'HH:mm:ss.SSS')),
      TIMESTAMP_TO_MILLIS(TIME_PARSE('15:20:30.230', 'HH:mm:ss.SSS'))
)