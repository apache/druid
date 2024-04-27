-- with the alias
select a2, max(distinct a2) over() as xyz from t2;
