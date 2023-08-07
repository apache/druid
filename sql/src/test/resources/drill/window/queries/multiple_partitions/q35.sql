select
        min(upper(c_varchar) || lower(c_varchar)) over(),
        max(upper(c_varchar) || upper(c_varchar)) over(),
        min(upper(c_varchar) || lower(c_varchar)) over(partition by c_date), 
        max(upper(c_varchar) || upper(c_varchar)) over(partition by c_date),
        min(upper(c_varchar) || lower(c_varchar)) over(partition by c_boolean, c_date order by 1),
        max(upper(c_varchar) || upper(c_varchar)) over(partition by c_date, c_date order by 1)
from
        j1;
