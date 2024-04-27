select t2.*, *, row_number() over(partition by b2 order by a2), t2.* from t2 order by row_number() over(partition by b2 order by a2);
