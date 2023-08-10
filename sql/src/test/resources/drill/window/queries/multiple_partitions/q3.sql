-- Same window clause
SELECT *
FROM
        (
        SELECT
                count(*) OVER (PARTITION BY c_date ORDER BY c_time) + sum(c_integer) OVER (PARTITION BY c_bigint ORDER BY c_time) AS total,
                count(*) OVER (PARTITION BY c_date ORDER BY c_time) AS count1,
                sum(c_integer) OVER (PARTITION BY c_date ORDER BY c_time) AS count2
        FROM j1
        ) sub
WHERE total <> count1 + count2;

