select *, t1.*, sum(a1) over w, count(a1) over w from t1 window w as (partition by b1 order by c1 desc);
