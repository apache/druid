---
layout: doc_page
title: "Tutorial: Updating existing data"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Tutorial: Updating existing data

This tutorial demonstrates how to update existing data, showing both overwrites and appends.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine. 

It will also be helpful to have finished [Tutorial: Loading a file](../tutorials/tutorial-batch.html), [Tutorial: Querying data](../tutorials/tutorial-query.html), and [Tutorial: Rollup](../tutorials/tutorial-rollup.html).

## Overwrite

This section of the tutorial will cover how to overwrite an existing interval of data.

### Load initial data

Let's load an initial data set which we will overwrite and append to.

The spec we'll use for this tutorial is located at `quickstart/tutorial/updates-init-index.json`. This spec creates a datasource called `updates-tutorial` from the `quickstart/tutorial/updates-data.json` input file.

Let's submit that task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-init-index.json 
```

We have three initial rows containing an "animal" dimension and "number" metric:

```bash
dsql> select * from "updates-tutorial"; 
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ tiger    │     1 │    100 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │     42 │
│ 2018-01-01T03:01:00.000Z │ giraffe  │     1 │  14124 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 3 rows in 1.42s.
```

### Overwrite the initial data

To overwrite this data, we can submit another task for the same interval, but with different input data.

The `quickstart/tutorial/updates-overwrite-index.json` spec will perform an overwrite on the `updates-tutorial` datasource.

Note that this task reads input from `quickstart/tutorial/updates-data2.json`, and `appendToExisting` is set to `false` (indicating this is an overwrite).

Let's submit that task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-overwrite-index.json 
```

When Druid finishes loading the new segment from this overwrite task, the "tiger" row now has the value "lion", the "aardvark" row has a different number, and the "giraffe" row has been replaced. It may take a couple of minutes for the changes to take effect:

```bash
dsql> select * from "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     1 │    100 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 3 rows in 0.02s.
```

## Combine old data with new data and overwrite

Let's try appending some new data to the `updates-tutorial` datasource now. We will add the data from `quickstart/tutorial/updates-data3.json`.

The `quickstart/tutorial/updates-append-index.json` task spec has been configured to read from the existing `updates-tutorial` datasource and the `quickstart/tutorial/updates-data3.json` file. The task will combine data from the two input sources, and then overwrite the original data with the new combined data.

Let's submit that task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-append-index.json 
```

When Druid finishes loading the new segment from this overwrite task, the new rows will have been added to the datasource. Note that roll-up occurred for the "lion" row:

```bash
dsql> select * from "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
│ 2018-01-01T05:01:00.000Z │ mongoose │     1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │     1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │     1 │    115 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 6 rows in 0.02s.
```

## Append to the data

Let's try another way of appending data.

The `quickstart/tutorial/updates-append-index2.json` task spec reads input from `quickstart/tutorial/updates-data4.json` and will append its data to the `updates-tutorial` datasource. Note that `appendToExisting` is set to `true` in this spec.

Let's submit that task:

```bash
bin/post-index-task --file quickstart/tutorial/updates-append-index2.json 
```

When the new data is loaded, we can see two additional rows after "octopus". Note that the new "bear" row with number 222 has not been rolled up with the existing bear-111 row, because the new data is held in a separate segment.

```bash
dsql> select * from "updates-tutorial";
┌──────────────────────────┬──────────┬───────┬────────┐
│ __time                   │ animal   │ count │ number │
├──────────────────────────┼──────────┼───────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │     2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │     1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    111 │
│ 2018-01-01T05:01:00.000Z │ mongoose │     1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │     1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │     1 │    115 │
│ 2018-01-01T04:01:00.000Z │ bear     │     1 │    222 │
│ 2018-01-01T09:01:00.000Z │ falcon   │     1 │   1241 │
└──────────────────────────┴──────────┴───────┴────────┘
Retrieved 8 rows in 0.02s.

```

If we run a GroupBy query instead of a `select *`, we can see that the "bear" rows will group together at query time:

```bash
dsql> select __time, animal, SUM("count"), SUM("number") from "updates-tutorial" group by __time, animal;
┌──────────────────────────┬──────────┬────────┬────────┐
│ __time                   │ animal   │ EXPR$2 │ EXPR$3 │
├──────────────────────────┼──────────┼────────┼────────┤
│ 2018-01-01T01:01:00.000Z │ lion     │      2 │    400 │
│ 2018-01-01T03:01:00.000Z │ aardvark │      1 │   9999 │
│ 2018-01-01T04:01:00.000Z │ bear     │      2 │    333 │
│ 2018-01-01T05:01:00.000Z │ mongoose │      1 │    737 │
│ 2018-01-01T06:01:00.000Z │ snake    │      1 │   1234 │
│ 2018-01-01T07:01:00.000Z │ octopus  │      1 │    115 │
│ 2018-01-01T09:01:00.000Z │ falcon   │      1 │   1241 │
└──────────────────────────┴──────────┴────────┴────────┘
Retrieved 7 rows in 0.23s.
```
