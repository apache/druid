-- empty over clause
explain plan for select sum(a1) over() from t1;
