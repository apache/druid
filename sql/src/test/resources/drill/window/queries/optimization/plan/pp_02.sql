-- empty over clause with expression
-- project is not below window, but Sean and I think it is OK
explain plan for select sum(a1+0) over() from t1;
