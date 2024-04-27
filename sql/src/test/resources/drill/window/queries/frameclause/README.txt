This directory has tests that verify correctness of frame clause use
within window definition in SQL queries.

Tests in this directory (i.e. frameclause) are divided into below groups, each
group is a directory and holds related test files.

(1) defaultFrame  - Tests RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
(2) multipl_wnwds - Tests to verify use of multiple window definitions and frame
                clauses in queries. 
(3) RBCRACR  - Tests for ROWS BETWEEN CURRENT ROW AND CURRENT ROW
(4) RBUPACR  - Tests for ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
(5) RBUPAUF  - Tests for RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
(6) subQueries - Subqueries with window definitions within & outside of subquery,
                 with frame clause in window definition.

CTAS used to create `t_alltype.parquet` file is,

0: jdbc:drill:schema=dfs.tmp> create table t_alltype as select
> case when columns[0] = '' then cast(null as integer) else cast(columns[0] as integer) end as c1,
> case when columns[1] = '' then cast(null as integer) else cast(columns[1] as integer) end as c2,
> case when columns[2] = '' then cast(null as bigint) else cast(columns[2] as bigint) end as c3,
> case when columns[3] = '' then cast(null as char(256)) else cast(columns[3] as char(256)) end as c4,
> case when columns[4] = '' then cast(null as varchar(256)) else cast(columns[4] as varchar(256)) end as c5,
> case when columns[5] = '' then cast(null as timestamp) else cast(columns[5] as timestamp) end as c6,
> case when columns[6] = '' then cast(null as date) else cast(columns[6] as date) end as c7,
> case when columns[7] = '' then cast(null as boolean) else cast(columns[7] as boolean) end as c8,
> case when columns[8] = '' then cast(null as double) else cast(columns[8] as double) end as c9
 > from dfs.tmp.`t_alltype.csv`;
+-----------+----------------------------+
| Fragment  | Number of records written  |
+-----------+----------------------------+
| 0_0       | 145                        |
+-----------+----------------------------+
1 row selected (1.565 seconds)

Postgres DDL to create table and load data into table

CREATE TABLE t_alltype(c1 INT, c2 INT, c3 BIGINT, c4 VARCHAR(256), c5 VARCHAR(256), c6 TIMESTAMP, c7 DATE, c8 BOOLEAN, c9 DOUBLE PRECISION);

COPY t_alltype FROM '$HOME/t_alltype.csv' DELIMITER ',' CSV NULL '';
