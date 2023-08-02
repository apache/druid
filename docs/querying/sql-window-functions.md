---
id: sql-window-functions
title: Window functions
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ License); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the SQL language.
>
> Window functions are an [experimental](../development/experimental.md) feature. Development and testing are still at early stage. Feel free to try window functions and provide your feedback.
> There are known issues where ORDER BY only works on ascending order and certain options may cause errors.
> Set the context parameter `windowsAreForClosers: true` to use window functions.

Window functions in Apache Druid produce values based upon the relationship of one row within a window of rows to the other rows within the same window. A window is a group of related rows within a result set. For example, rows with the same value for a specific dimension.

The following example groups results with the same `channel` value into windows. For each window, the query returns the rank of each row in ascending order based upon its `delta` value.

```sql
SELECT FLOOR(__time TO DAY) AS event_time,
    channel,
    ABS(delta) AS change,
    RANK() OVER w AS rank_value
FROM wikipedia
WHERE channel in ('`#kk.wikipedia`', '#lt.wikipedia')
AND '2016-06-28' > FLOOR(__time TO DAY) > '2016-06-26'
GROUP BY channel, ABS(delta), __time
WINDOW w AS (PARTITION BY channel ORDER BY ABS(delta) ASC)
```

<details>
<summary> View results </summary>

| `event_time` | `channel` | `change`| `rank_value` |
| -- | -- | -- | -- |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 1	| 1 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 1	| 1 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 7	| 3 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 56 | 4 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 56 | 4 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 63 | 6 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 91 | 7 |  
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 2440 | 8 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 2703 | 9 |
| `2016-06-27T00:00:00.000Z`	| `#kk.wikipedia`	| 6900 |10 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 1	| 1 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 2	| 2 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 13 | 3 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 28 | 4 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 53 | 5 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 56 | 6 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 59 | 7 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 391 | 8 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 894	| 9 |
| `2016-06-27T00:00:00.000Z`	| `#lt.wikipedia`| 4358 | 10 |


</details>

Window functions are similar to [aggregation functions](./aggregations.md).  

You can use the OVER clause to treat other Druid aggregation functions as window functions. For example, the sum of a value for rows within a window.

When working with window functions, consider the following:
- Window functions only work on GROUP BY queries.
- Window functions support aliasing.

## Define a window with the OVER clause

The OVER clause defines the query windows for window functions as follows:
- PARTITION BY indicates the dimension that defines the rows within the window
- ORDER BY specifies the order of the rows within the windows. Currently only ASC, ascending, order works.
Sometimes windows are called partitions, but don't confuse them with the segment partitioning feature of Druid.


The following OVER clause example sets the window dimension to `channel` and orders the results by the absolute value of `delta` ascending:

```sql
...
RANK() OVER (PARTITION BY channel ORDER BY ABS(delta) ASC)
...
```

## Window function reference

|Function|Notes|
|--------|-----|
| `ROW_NUMBER()`| Returns the number of the row within the window|
|`RANK()`| Returns the rank for a row within a window | 
|`DENSE_RANK()`| Returns the rank for a row within a window without gaps. For example, if two rows tie for rank of 1, the subsequent row is ranked 2. |
|`PERCENT_RANK()`| Returns the rank of the row calculated as a percentage according to the formula: `(rank - 1) / (total window rows - 1)` |
|`CUME_DIST()`| Returns the cumulative distribution of the current row within the window calculated as `number of window rows at the same rank or higher than current row` / `total window rows` |
|`NTILE(tiles)`| Divides the rows within a window as evenly as possible into the number of tiles, also called buckets, and returns the value of the tile that the row falls into  |None |
|`LAG(expr, offset, default)`| Returns the value evaluated at the row that precedes the current row by the offset number within the window; if there is no such row, returns the given default value |
|`LEAD(expr, offset, default)`| Returns the value evaluated at the row that follows the current row by the offset number within the window; if there is no such row, returns the given default value |
|`FIRST_VALUE(expr)`| Returns the value for the expression for the first row within the window|
|`LAST_VALUE(expr)`| Returns the value for the expression for the last row within the window |


## Example

The following example illustrates all of the built-in window functions to compare the number of characters changed per event for a channel in the Wikipedia data set.

```sql
SELECT FLOOR(__time TO DAY) AS event_time,
    channel,
    ABS(delta) AS change,
    ROW_NUMBER() OVER w AS row_no,
    RANK() OVER w AS rank_no,
    DENSE_RANK() OVER w AS dense_rank_no,
    PERCENT_RANK() OVER w AS pct_rank,
    CUME_DIST() OVER w AS cumulative_dist,
    NTILE(4) OVER w AS ntile_val,
    LAG(ABS(delta), 1, 0) OVER w AS lag_val,
    LEAD(ABS(delta), 1, 0) OVER w AS lead_val,
    FIRST_VALUE(ABS(delta)) OVER w AS first_val,
    LAST_VALUE(ABS(delta)) OVER w AS last_val
FROM wikipedia
WHERE channel IN ('`#kk.wikipedia`', '#lt.wikipedia')
GROUP BY channel, ABS(delta), FLOOR(__time TO DAY) 
WINDOW w AS (PARTITION BY channel ORDER BY ABS(delta) ASC)
```

<details>
<summary> View results </summary>

|`event_time`|`channel`|`change`|`row_no`|`rank_no`|`dense_rank_no`|`pct_rank`|`cumulative_dist`|`ntile_val`|`lag_val`|`lead_val`|`first_val`|`last_val`|
|------------|---------|--------|--------|---------|---------------|----------|----------------|-----------|---------|----------|-----------|----------|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|1|1|1|1|0|0.125|1|null|7|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|7|2|2|2|0.14285714285714285|0.25|1|1|56|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|56|3|3|3|0.2857142857142857|0.375|2|7|63|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|63|4|4|4|0.42857142857142855|0.5|2|56|91|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|91|5|5|5|0.5714285714285714|0.625|3|63|2440|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|2440|6|6|6|0.7142857142857143|0.75|3|91|2703|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|2703|7|7|7|0.8571428571428571|0.875|4|2440|6900|1|6900|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|6900|8|8|8|1|1|4|2703|null|1|6900|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|1|1|1|1|0|0.1|1|null|2|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|2|2|2|2|0.1111111111111111|0.2|1|1|13|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|13|3|3|3|0.2222222222222222|0.3|1|2|28|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|28|4|4|4|0.3333333333333333|0.4|2|13|53|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|53|5|5|5|0.4444444444444444|0.5|2|28|56|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|56|6|6|6|0.5555555555555556|0.6|2|53|59|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|59|7|7|7|0.6666666666666666|0.7|3|56|391|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|391|8|8|8|0.7777777777777778|0.8|3|59|894|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|894|9|9|9|0.8888888888888888|0.9|4|391|4358|1|4358|
|`2016-06-27T00:00:00.000Z`| `#lt.wikipedia`|4358|10|10|10|1|1|4|894|null|1|4358|

</details>