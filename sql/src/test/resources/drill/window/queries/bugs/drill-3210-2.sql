select *, avg(a1) over() from t1 order by avg(a1) over();
