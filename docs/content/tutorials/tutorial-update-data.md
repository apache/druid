---
layout: doc_page
---

# Tutorial: Updating existing data

This tutorial demonstrates how to update existing data, showing both overwrites and appends.

For this tutorial, we'll assume you've already downloaded Druid as described in 
the [single-machine quickstart](index.html) and have it running on your local machine. 

It will also be helpful to have finished [Tutorial: Loading a file](../tutorials/tutorial-batch.html), [Tutorial: Querying data](../tutorials/tutorial-query.html), and [Tutorial: Rollup](../tutorials/tutorial-rollup.html).

## Overwrite

This section of the tutorial will cover how to overwrite an existing interval of data.

### Load initial data

Let's load an initial data set which we will overwrite and append to.

The spec we'll use for this tutorial is located at `examples/updates-init-index.json`. This spec creates a datasource called `updates-tutorial` from the `examples/updates-data.json` input file.

Let's submit that task:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-init-index.json http://localhost:8090/druid/indexer/v1/task
```

We have three initial rows containing an "animal" dimension and "number" metric:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-select-sql.json http://localhost:8082/druid/v2/sql
```

```json
[
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "animal": "tiger",
    "count": 1,
    "number": 100
  },
  {
    "__time": "2018-01-01T03:01:00.000Z",
    "animal": "aardvark",
    "count": 1,
    "number": 42
  },
  {
    "__time": "2018-01-01T03:01:00.000Z",
    "animal": "giraffe",
    "count": 1,
    "number": 14124
  }
]
```

### Overwrite the initial data

To overwrite this data, we can submit another task for the same interval, but with different input data.

The `examples/updates-overwrite-index.json` spec will perform an overwrite on the `updates-tutorial` datasource.

Note that this task reads input from `examples/updates-data2.json`, and `appendToExisting` is set to `false` (indicating this is an overwrite).

Let's submit that task:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-overwrite-index.json http://localhost:8090/druid/indexer/v1/task
```

When Druid finishes loading the new segment from this overwrite task, the "tiger" row now has the value "lion", the "aardvark" row has a different number, and the "giraffe" row has been replaced. It may take a couple of minutes for the changes to take effect:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-select-sql.json http://localhost:8082/druid/v2/sql
```

```json
[
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "animal": "lion",
    "count": 1,
    "number": 100
  },
  {
    "__time": "2018-01-01T03:01:00.000Z",
    "animal": "aardvark",
    "count": 1,
    "number": 9999
  },
  {
    "__time": "2018-01-01T04:01:00.000Z",
    "animal": "bear",
    "count": 1,
    "number": 111
  }
]

```

## Append to the data

Let's try appending data.

The `examples/updates-append-index.json` task spec reads input from `examples/updates-data3.json` and will append its data to the `updates-tutorial` datasource. Note that `appendToExisting` is set to `true` in this spec.

Let's submit that task:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-append-index.json http://localhost:8090/druid/indexer/v1/task
```

When the new data is loaded, we can see two additional rows after "octopus". Note that the new "bear" row with number 222 has not been rolled up with the existing bear-111 row, because the new data is held in a separate segment. The same applies to the two "lion" rows.

```json
[
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "animal": "lion",
    "count": 1,
    "number": 100
  },
  {
    "__time": "2018-01-01T03:01:00.000Z",
    "animal": "aardvark",
    "count": 1,
    "number": 9999
  },
  {
    "__time": "2018-01-01T04:01:00.000Z",
    "animal": "bear",
    "count": 1,
    "number": 111
  },
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "animal": "lion",
    "count": 1,
    "number": 300
  },
  {
    "__time": "2018-01-01T04:01:00.000Z",
    "animal": "bear",
    "count": 1,
    "number": 222
  },
  {
    "__time": "2018-01-01T05:01:00.000Z",
    "animal": "mongoose",
    "count": 1,
    "number": 737
  },
  {
    "__time": "2018-01-01T06:01:00.000Z",
    "animal": "snake",
    "count": 1,
    "number": 1234
  },
  {
    "__time": "2018-01-01T07:01:00.000Z",
    "animal": "octopus",
    "count": 1,
    "number": 115
  },
  {
    "__time": "2018-01-01T09:01:00.000Z",
    "animal": "falcon",
    "count": 1,
    "number": 1241
  }
]
```

If we run a GroupBy query instead of a `select *`, we can see that the separate "lion" and "bear" rows will group together at query time:

`select __time, animal, SUM("count"), SUM("number") from "updates-tutorial" group by __time, animal;`

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/updates-groupby-sql.json http://localhost:8082/druid/v2/sql
```

```json
[
  {
    "__time": "2018-01-01T01:01:00.000Z",
    "animal": "lion",
    "EXPR$2": 2,
    "EXPR$3": 400
  },
  {
    "__time": "2018-01-01T03:01:00.000Z",
    "animal": "aardvark",
    "EXPR$2": 1,
    "EXPR$3": 9999
  },
  {
    "__time": "2018-01-01T04:01:00.000Z",
    "animal": "bear",
    "EXPR$2": 2,
    "EXPR$3": 333
  },
  {
    "__time": "2018-01-01T05:01:00.000Z",
    "animal": "mongoose",
    "EXPR$2": 1,
    "EXPR$3": 737
  },
  {
    "__time": "2018-01-01T06:01:00.000Z",
    "animal": "snake",
    "EXPR$2": 1,
    "EXPR$3": 1234
  },
  {
    "__time": "2018-01-01T07:01:00.000Z",
    "animal": "octopus",
    "EXPR$2": 1,
    "EXPR$3": 115
  },
  {
    "__time": "2018-01-01T09:01:00.000Z",
    "animal": "falcon",
    "EXPR$2": 1,
    "EXPR$3": 1241
  }
]
```