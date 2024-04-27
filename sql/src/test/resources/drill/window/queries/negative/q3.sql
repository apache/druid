select a2, count(distinct b2) over(partition by a2) from t2;
