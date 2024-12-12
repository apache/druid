select col6 , lead(col6) over(partition by col7 order by col6) lead_col6
from "allTypsUniq.parquet"
where col6 IN (
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1952-08-14', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1981-03-14', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1947-05-12', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1995-10-09', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1968-02-03', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1943-02-02', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1957-12-10', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('2015-01-01', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('2013-01-03', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1989-02-02', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('2007-10-01', 'yyyy-MM-dd')) ,
    TIMESTAMP_TO_MILLIS(TIME_PARSE('1999-01-03', 'yyyy-MM-dd'))
)