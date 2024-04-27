select rank() over(order by row_number() over(order by c1)) from t1;
