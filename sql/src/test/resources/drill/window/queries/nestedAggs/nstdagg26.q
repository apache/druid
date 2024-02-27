SELECT count(count(distinct col7)) over (PARTITION BY col7), col7 from "allTypsUniq.parquet" GROUP BY col7
