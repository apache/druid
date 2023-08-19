-- multiple functions
-- partition by only
-- with expression in window function
-- window functions used in expression
select sum(a1) over(partition by c1) + sum(a1+100) over(partition by c1) from t1;
