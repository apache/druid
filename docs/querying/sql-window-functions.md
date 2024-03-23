---
id: sql-window-functions
title: Window functions
description: Reference for window functions
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

:::info

Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
This document describes the SQL language.

Window functions are an [experimental](../development/experimental.md) feature.
Development and testing are still at early stage. Feel free to try window functions and provide your feedback.
Windows functions are not currently supported by multi-stage-query engine so you cannot use them in SQL-based ingestion. 


Set the context parameter `enableWindowing: true` to use window functions.

:::

Window functions in Apache Druid produce values based upon the relationship of one row within a window of rows to the other rows within the same window. A window is a group of related rows within a result set. For example, rows with the same value for a specific dimension.

Window functions in Druid require a GROUP BY statement. Druid performs the row-level aggregations for the GROUP BY before performing the window function calculations.

The following example organizes results with the same `channel` value into windows. For each window, the query returns the rank of each row in ascending order based upon its `changed` value.

```sql
SELECT FLOOR(__time TO DAY) AS event_time,
    channel,
    ABS(delta) AS change,
    RANK() OVER w AS rank_value
FROM wikipedia
WHERE channel in ('#kk.wikipedia', '#lt.wikipedia')
AND '2016-06-28' > FLOOR(__time TO DAY) > '2016-06-26'
GROUP BY channel, ABS(delta), __time
WINDOW w AS (PARTITION BY channel ORDER BY ABS(delta) ASC)
```

<details>
<summary> View results </summary>

| `event_time` | `channel` | `change`| `rank_value` |
| -- | -- | -- | -- |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 1 | 1 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 1 | 1 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 7 | 3 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 56 | 4 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 56 | 4 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 63 | 6 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 91 | 7 |  
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 2440 | 8 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 2703 | 9 |
| `2016-06-27T00:00:00.000Z`| `#kk.wikipedia`| 6900 |10 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 1 | 1 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 2 | 2 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 13 | 3 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 28 | 4 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 53 | 5 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 56 | 6 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 59 | 7 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 391 | 8 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 894 | 9 |
| `2016-06-27T00:00:00.000Z`| `#lt.wikipedia`| 4358 | 10 |

</details>

Window functions are similar to [aggregation functions](./aggregations.md).  

You can use the OVER clause to treat other Druid aggregation functions as window functions. For example, the sum of a value for rows within a window.

Window functions support aliasing.

## Window function syntax

You can write a window function in Druid using either syntax below.
The second syntax shows a window alias to reference a window that you can reuse.


```sql
window_function() OVER (
  [PARTITION BY partitioning expression]
  [ORDER BY order expression]
  [[ROWS, RANGE] BETWEEN range start AND range end])
FROM table
GROUP BY dimensions
```

```sql
window_function() OVER w
FROM table
WINDOW w AS ([PARTITION BY partitioning expression] [ORDER BY order expression]
  [[ROWS, RANGE] BETWEEN range start AND range end])
GROUP BY dimensions
```

The OVER clause defines the query windows for window functions as follows:
- PARTITION BY indicates the dimension that defines window boundaries
- ORDER BY specifies the order of the rows within the windows

An empty OVER clause or the absence of a PARTITION BY clause indicates that all data belongs to a single window.

In the following example, the following OVER clause example sets the window dimension to `channel` and orders the results by the absolute value of `delta` ascending:

```sql
...
RANK() OVER (PARTITION BY channel ORDER BY ABS(delta) ASC)
...
```

Window frames, set in ROWS and RANGE expressions, limit the set of rows used for the windowed aggregation.

ROWS and RANGE accept the following values for `range start` and `range end`:
- UNBOUNDED PRECEDING: from the beginning of the window as ordered by the order expression
- _N_ ROWS PRECEDING: _N_ rows before the current row as ordered by the order expression
- CURRENT ROW: the current row
- _N_ ROWS FOLLOWING: _N_ rows after the current row as ordered by the order expression
- UNBOUNDED FOLLOWING: to the end of the window as ordered by the order expression

See [Example with window frames](#example-with-window-frames) for more detail.
 
Druid applies the GROUP BY dimensions before calculating all non-window aggregation functions. Then it applies the window function over the aggregated results.

:::note

Sometimes windows are called partitions. However, the partitioning for window functions are a shuffle (partition) of the result set created at query time and is not to be confused with Druid's segment partitioning feature which partitions data at ingest time.

:::

### ORDER BY windows

When the window definition only specifies ORDER BY and not PARTITION BY, it sorts the aggregate data set and applies the function in that order.

The following query uses `ORDER BY SUM(delta) DESC` to rank user hourly activity from the most changed the least changed within an hour:

```sql
SELECT
    TIME_FLOOR(__time, 'PT1H') as time_hour, 
    channel, 
    user,
    SUM(delta) net_user_changes,
    RANK() OVER (ORDER BY SUM(delta) DESC) AS editing_rank
FROM "wikipedia"
WHERE channel IN ('#kk.wikipedia', '#lt.wikipedia')
  AND __time BETWEEN '2016-06-27' AND '2016-06-28'
GROUP BY TIME_FLOOR(__time, 'PT1H'), channel, user
ORDER BY 5 
```

<details>
<summary> View results </summary>

| `time_hour` | `channel` | `user` | `net_user_changes` | `editing_rank` |
| --- | --- | --- | --- | --- |
| `2016-06-27T15:00:00.000Z` | `#kk.wikipedia` | `Nurkhan` | 6900| 1 |
| `2016-06-27T19:00:00.000Z` | `#lt.wikipedia` | `77.221.66.41` | 4358 | 2 |
| `2016-06-27T09:00:00.000Z` | `#kk.wikipedia` | `Салиха` | 2702 | 3 |
| `2016-06-27T04:00:00.000Z` | `#kk.wikipedia` | `Nurkhan` | 2440 | 4 |
| `2016-06-27T09:00:00.000Z` | `#lt.wikipedia` | `80.4.147.222` | 894 | 5 |
| `2016-06-27T09:00:00.000Z` | `#lt.wikipedia` | `178.11.203.212` | 447 | 6 |
| `2016-06-27T11:00:00.000Z` | `#kk.wikipedia` | `Нұрлан Рахымжанов` | 126 | 7 |
| `2016-06-27T06:00:00.000Z` | `#kk.wikipedia` | `Шокай` | 91 | 8 |
| `2016-06-27T11:00:00.000Z` | `#lt.wikipedia` | `MaryroseB54` | 59 | 9 |
| `2016-06-27T04:00:00.000Z` | `#kk.wikipedia` | `Нұрлан Рахымжанов` | 56 | 10 |
| `2016-06-27T12:00:00.000Z` | `#lt.wikipedia` | `Karoliuk` | 53 | 11 |
| `2016-06-27T12:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | 28 | 12 |
| `2016-06-27T07:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | 13 | 13 |
| `2016-06-27T10:00:00.000Z` | `#lt.wikipedia` | `80.4.147.222` | 1 | 14 |
| `2016-06-27T07:00:00.000Z` | `#kk.wikipedia` | `Салиха` | -1 | 15 |
| `2016-06-27T06:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | -2 | 16 |
</details>

### PARTITION BY windows

When a window only specifies PARTITION BY partition expression, Druid calculates the aggregate window function over all the rows that share a value within the selected dataset.

The following example demonstrates a query that uses two different windows—`PARTITION BY channel` and `PARTITION BY user`—to calculate the total activity in the channel and total activity by the user so that they can be compared to individual hourly activity:

```sql
SELECT
    TIME_FLOOR(__time, 'PT1H') as time_hour,
    channel,
    user,
    SUM(delta) AS hourly_user_changes,
    SUM(SUM(delta)) OVER (PARTITION BY user) AS total_user_changes,
    SUM(SUM(delta)) OVER (PARTITION BY channel) AS total_channel_changes
FROM "wikipedia"
WHERE channel IN ('#kk.wikipedia', '#lt.wikipedia')
  AND __time BETWEEN '2016-06-27' AND '2016-06-28'
GROUP BY TIME_FLOOR(__time, 'PT1H'), 2, 3
ORDER BY channel, TIME_FLOOR(__time, 'PT1H'), user
```

<details>
<summary> View results </summary>

| `time_hour` | `channel` | `user` | `hourly_user_changes` | `total_user_changes` | `total_channel_changes` |
| --- | ---| ---| --- | --- | --- |
| `2016-06-27T04:00:00.000Z` | `#kk.wikipedia` | `Nurkhan` | 2440 | 9340 | 12314 |
| `2016-06-27T04:00:00.000Z` | `#kk.wikipedia` | `Нұрлан Рахымжанов` | 56 | 182 | 12314 |
| `2016-06-27T06:00:00.000Z` | `#kk.wikipedia` | `Шокай` | 91 | 91 | 12314 |
| `2016-06-27T07:00:00.000Z` | `#kk.wikipedia` | `Салиха` | -1 | 2701 | 12314 |
| `2016-06-27T09:00:00.000Z` | `#kk.wikipedia` | `Салиха` | 2702 | 2701 | 12314 |
| `2016-06-27T11:00:00.000Z` | `#kk.wikipedia` | `Нұрлан Рахымжанов` | 126 | 182 | 12314 |
| `2016-06-27T15:00:00.000Z` | `#kk.wikipedia` | `Nurkhan` | 6900 | 9340 | 12314 |
| `2016-06-27T06:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | -2 | 39 | 5851 |
| `2016-06-27T07:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | 13 | 39 | 5851 |
| `2016-06-27T09:00:00.000Z` | `#lt.wikipedia` | `178.11.203.212` | 447 | 447 | 5851 |
| `2016-06-27T09:00:00.000Z` | `#lt.wikipedia` | `80.4.147.222` | 894 | 895 | 5851 |
| `2016-06-27T10:00:00.000Z` | `#lt.wikipedia` | `80.4.147.222` | 1 | 895 | 5851 |
| `2016-06-27T11:00:00.000Z` | `#lt.wikipedia` | `MaryroseB54` | 59 | 59 | 5851 |
| `2016-06-27T12:00:00.000Z` | `#lt.wikipedia` | `Karoliuk` | 53 | 53 | 5851 |
| `2016-06-27T12:00:00.000Z` | `#lt.wikipedia` | `Powermelon` | 28 | 39 | 5851 |
| `2016-06-27T19:00:00.000Z` | `#lt.wikipedia` | `77.221.66.41` | 4358 | 4358 | 5851 |

</details>

In this example, the dataset is filtered for a single day. Therefore the window function results represent the total activity for the day, for the `user` and for the `channel` dimensions respectively.

This type of result helps you analyze the impact of an individual user's hourly activity:
- the impact to the channel by comparing `hourly_user_changes` to `total_channel_changes`
- the impact of each user over the channel by `total_user_changes` to `total_channel_changes`
- the progress of each user's individual activity by comparing `hourly_user_changes` to `total_user_changes`

#### Window frame guardrails

Druid has guardrail logic to prevent you from executing window function queries with window frame expressions that might return unexpected results.

For example:
- You cannot set expressions as bounds for window frames.
- You cannot use two FOLLOWING expressions in the window frame. For example: `ROWS BETWEEN 2 ROWS FOLLOWING and 3 ROWS FOLLOWING`.
- You can only use a RANGE frames when both endpoints are unbounded or current row.

If you write a query that violates one of these conditions, Druid throws an error: "The query contains a window frame which may return incorrect results. To disregard this warning, set `windowingStrictValidation` to false in the query context."

## Window function reference

|Function|Notes|
|--------|-----|
| `ROW_NUMBER()` | Returns the number of the row within the window starting from 1 |
| `RANK()` | Returns the rank with gaps for a row within a window. For example, if two rows tie for rank 1, the next rank is 3 | 
| `DENSE_RANK()` | Returns the rank for a row within a window without gaps. For example, if two rows tie for rank of 1, the subsequent row is ranked 2. |
| `PERCENT_RANK()` | Returns the relative rank of the row calculated as a percentage according to the formula: `RANK() OVER (window) / COUNT(1) OVER (window)` |
| `CUME_DIST()` | Returns the cumulative distribution of the current row within the window calculated as number of window rows at the same rank or higher than current row divided by total window rows. The return value ranges between `1/number of rows` and 1 |
| `NTILE(tiles)` | Divides the rows within a window as evenly as possible into the number of tiles, also called buckets, and returns the value of the tile that the row falls into | None |
| `LAG(expr[, offset])` | If you do not supply an `offset`, returns the value evaluated at the row preceding the current row. Specify an offset number, `n`, to return the value evaluated at `n` rows preceding the current one |
| `LEAD(expr[, offset])` | If you do not supply an `offset`, returns the value evaluated at the row following the current row. Specify an offset number `n` to return the value evaluated at `n` rows following the current one; if there is no such row, returns the given default value |
| `FIRST_VALUE(expr)` | Returns the value evaluated for the expression for the first row within the window |
| `LAST_VALUE(expr)` | Returns the value evaluated for the expression for the last row within the window |

## Examples

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
WHERE channel IN ('#kk.wikipedia', '#lt.wikipedia')
GROUP BY channel, ABS(delta), FLOOR(__time TO DAY) 
WINDOW w AS (PARTITION BY channel ORDER BY ABS(delta) ASC)
```

<details>
<summary> View results </summary>

|`event_time`|`channel`|`change`|`row_no`|`rank_no`|`dense_rank_no`|`pct_rank`|`cumulative_dist`|`ntile_val`|`lag_val`|`lead_val`|`first_val`|`last_val`|
|------------|---------|--------|--------|---------|---------------|----------|----------------|-----------|---------|----------|-----------|----------|
|`2016-06-27T00:00:00.000Z`|`#kk.wikipedia`|1|1|1|1|0.0|0.125|1|null|7|1|6900|
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

The following example demonstrates applying the SUM() function over the values in a window to calculate the cumulative changes to a channel over time:

```sql
SELECT
    FLOOR(__time TO MINUTE) as "time",
    channel,
    ABS(delta) AS changes,
    sum(ABS(delta)) OVER (PARTITION BY channel ORDER BY FLOOR(__time TO MINUTE) ASC) AS cum_changes
FROM wikipedia
WHERE channel IN ('#kk.wikipedia', '#lt.wikipedia')
GROUP BY channel, __time, delta
```

<details>
<summary> View results </summary>

|`time`|`channel`|`changes`|`cum_changes`|
|------|---------|---------|-------------|
|`2016-06-27T04:20:00.000Z`|`#kk.wikipedia`|56|56|
|`2016-06-27T04:35:00.000Z`|`#kk.wikipedia`|2440|2496|
|`2016-06-27T06:15:00.000Z`|`#kk.wikipedia`|91|2587|
|`2016-06-27T07:32:00.000Z`|`#kk.wikipedia`|1|2588|
|`2016-06-27T09:00:00.000Z`|`#kk.wikipedia`|2703|5291|
|`2016-06-27T09:24:00.000Z`|`#kk.wikipedia`|1|5292|
|`2016-06-27T11:00:00.000Z`|`#kk.wikipedia`|63|5355|
|`2016-06-27T11:05:00.000Z`|`#kk.wikipedia`|7|5362|
|`2016-06-27T11:32:00.000Z`|`#kk.wikipedia`|56|5418|
|`2016-06-27T15:21:00.000Z`|`#kk.wikipedia`|6900|12318|
|`2016-06-27T06:17:00.000Z`|`#lt.wikipedia`|2|2|
|`2016-06-27T07:55:00.000Z`|`#lt.wikipedia`|13|15|
|`2016-06-27T09:05:00.000Z`|`#lt.wikipedia`|894|909|
|`2016-06-27T09:12:00.000Z`|`#lt.wikipedia`|391|1300|
|`2016-06-27T09:23:00.000Z`|`#lt.wikipedia`|56|1356|
|`2016-06-27T10:59:00.000Z`|`#lt.wikipedia`|1|1357|
|`2016-06-27T11:49:00.000Z`|`#lt.wikipedia`|59|1416|
|`2016-06-27T12:41:00.000Z`|`#lt.wikipedia`|53|1469|
|`2016-06-27T12:58:00.000Z`|`#lt.wikipedia`|28|1497|
|`2016-06-27T19:03:00.000Z`|`#lt.wikipedia`|4358|5855|

</details>

### Example with window frames

The following query uses a few different window frames to calculate overall activity by channel:

```sql
SELECT
    channel, 
    TIME_FLOOR(__time, 'PT1H')      AS time_hour, 
    SUM(delta)                      AS hourly_channel_changes,
    SUM(SUM(delta)) OVER cumulative AS cumulative_activity_in_channel,
    SUM(SUM(delta)) OVER moving5    AS csum5,
    COUNT(1) OVER moving5           AS count5
FROM "wikipedia"
WHERE channel = '#en.wikipedia'
  AND __time BETWEEN '2016-06-27' AND '2016-06-28'
GROUP BY 1, TIME_FLOOR(__time, 'PT1H')
WINDOW cumulative AS (   
                         PARTITION BY channel 
                         ORDER BY TIME_FLOOR(__time, 'PT1H') 
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     )
                     ,
        moving5 AS ( 
                    PARTITION BY channel 
                    ORDER BY TIME_FLOOR(__time, 'PT1H') 
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                  )
```

<details>
<summary> View results </summary>

| `channel` | `time_hour` | `hourly_channel_changes` | `cumulative_activity_in_channel` | `csum5` | `count5` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `#en.wikipedia` | `2016-06-27T00:00:00.000Z` | 74996 | 74996 | 74996 | 1 |
| `#en.wikipedia` | `2016-06-27T01:00:00.000Z` | 24150 | 99146 | 99146 | 2 |
| `#en.wikipedia` | `2016-06-27T02:00:00.000Z` | 102372 | 201518 | 201518 | 3 |
| `#en.wikipedia` | `2016-06-27T03:00:00.000Z` | 61362 | 262880 | 262880 | 4 |
| `#en.wikipedia` | `2016-06-27T04:00:00.000Z` | 61666 | 324546 | 324546 | 5 |
| `#en.wikipedia` | `2016-06-27T05:00:00.000Z` | 144199 | 468745 | 393749 | 5 |
| `#en.wikipedia` | `2016-06-27T06:00:00.000Z` | 33414 | 502159 | 403013 | 5 |
| `#en.wikipedia` | `2016-06-27T07:00:00.000Z` | 79397 | 581556 | 380038 | 5 |
| `#en.wikipedia` | `2016-06-27T08:00:00.000Z` | 104436 | 685992 | 423112 | 5 |
| `#en.wikipedia` | `2016-06-27T09:00:00.000Z` | 58020 | 744012 | 419466 | 5 |
| `#en.wikipedia` | `2016-06-27T10:00:00.000Z` | 93904 | 837916 | 369171 | 5 |
| `#en.wikipedia` | `2016-06-27T11:00:00.000Z` | 74436 | 912352 | 410193 | 5 |
| `#en.wikipedia` | `2016-06-27T12:00:00.000Z` | 83491 | 995843 | 414287 | 5 |
| `#en.wikipedia` | `2016-06-27T13:00:00.000Z` | 103051 | 1098894 | 412902 | 5 |
| `#en.wikipedia` | `2016-06-27T14:00:00.000Z` | 211411 | 1310305 | 566293 | 5 |
| `#en.wikipedia` | `2016-06-27T15:00:00.000Z` | 101247 | 1411552 | 573636 | 5 |
| `#en.wikipedia` | `2016-06-27T16:00:00.000Z` | 189765 | 1601317 | 688965 | 5 |
| `#en.wikipedia` | `2016-06-27T17:00:00.000Z` | 74404 | 1675721 | 679878 | 5 |
| `#en.wikipedia` | `2016-06-27T18:00:00.000Z` | 104824 | 1780545 | 681651 | 5 |
| `#en.wikipedia` | `2016-06-27T19:00:00.000Z` | 71268 | 1851813 | 541508 | 5 |
| `#en.wikipedia` | `2016-06-27T20:00:00.000Z` | 88185 | 1939998 | 528446 | 5 |
| `#en.wikipedia` | `2016-06-27T21:00:00.000Z` | 42584 | 1982582 | 381265 | 5 |

</details>

The example defines multiple window specifications in the WINDOW clause that you can use for various window function calculations.

The query uses two windows:
- `cumulative` is partitioned by channel and includes all rows from the beginning of partition up to the current row as ordered by `__time` to enable cumulative aggregation
- `moving5` is also partitioned by channel but only includes up to the last four rows and the current row as ordered by time

The number of rows considered for the `moving5` window for the `count5` column:
- starts at a single row because there are no rows before the current one
- grows up to five rows as defined by `ROWS BETWEEN 4 ROWS PRECEDING AND CURRENT ROW`

## Known issues

The following are known issues with window functions:

-  Aggregates with ORDER BY specified are processed in the window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  
     This behavior differs from other databases that use the default of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.  
     In cases where the order column is unique there is no difference between RANGE / ROWS; windows with RANGE specifications are handled as ROWS.
- LEAD/LAG ignores the default value
- LAST_VALUE returns the last value of the window even when you include an ORDER BY clause
