---
id: having
title: "Having filters (groupBy)"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the native
> language. For information about functions available in SQL, refer to the
> [SQL documentation](sql.md#scalar-functions).

A having clause is a JSON object identifying which rows from a groupBy query should be returned, by specifying conditions on aggregated values.

It is essentially the equivalent of the HAVING clause in SQL.

Apache Druid supports the following types of having clauses.

### Query filters

Query filter HavingSpecs allow all [Druid query filters](filters.html) to be used in the Having part of the query.

The grammar for a query filter HavingSpec is:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type" : "filter",
            "filter" : <any Druid query filter>
        }
}
```

For example, to use a selector filter:


```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type" : "filter",
            "filter" : {
              "type": "selector",
              "dimension" : "<dimension>",
              "value" : "<dimension_value>"
            }
        }
}
```

You can use "filter" HavingSpecs to filter on the timestamp of result rows by applying a filter to the "\_\_time"
column.

### Numeric filters

The simplest having clause is a numeric filter.
Numeric filters can be used as the base filters for more complex boolean expressions of filters.

Here's an example of a having-clause numeric filter:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "greaterThan",
            "aggregation": "<aggregate_metric>",
            "value": <numeric_value>
        }
}
```

#### Equal To

The equalTo filter will match rows with a specific aggregate value.
The grammar for an `equalTo` filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "equalTo",
            "aggregation": "<aggregate_metric>",
            "value": <numeric_value>
        }
}
```

This is the equivalent of `HAVING <aggregate> = <value>`.

#### Greater Than

The greaterThan filter will match rows with aggregate values greater than the given value.
The grammar for a `greaterThan` filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "greaterThan",
            "aggregation": "<aggregate_metric>",
            "value": <numeric_value>
        }
}
```

This is the equivalent of `HAVING <aggregate> > <value>`.

#### Less Than

The lessThan filter will match rows with aggregate values less than the specified value.
The grammar for a `greaterThan` filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "lessThan",
            "aggregation": "<aggregate_metric>",
            "value": <numeric_value>
        }
}
```

This is the equivalent of `HAVING <aggregate> < <value>`.



### Dimension Selector Filter

#### dimSelector

The dimSelector filter will match rows with dimension values equal to the specified value.
The grammar for a `dimSelector` filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
       {
            "type": "dimSelector",
            "dimension": "<dimension>",
            "value": <dimension_value>
        }
}
```


### Logical expression filters

#### AND

The grammar for an AND filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "and",
            "havingSpecs": [
                {
                    "type": "greaterThan",
                    "aggregation": "<aggregate_metric>",
                    "value": <numeric_value>
                },
                {
                    "type": "lessThan",
                    "aggregation": "<aggregate_metric>",
                    "value": <numeric_value>
                }
            ]
        }
}
```

#### OR

The grammar for an OR filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
            "type": "or",
            "havingSpecs": [
                {
                    "type": "greaterThan",
                    "aggregation": "<aggregate_metric>",
                    "value": <numeric_value>
                },
                {
                    "type": "equalTo",
                    "aggregation": "<aggregate_metric>",
                    "value": <numeric_value>
                }
            ]
        }
}
```

#### NOT

The grammar for a NOT filter is as follows:

```json
{
    "queryType": "groupBy",
    "dataSource": "sample_datasource",
    ...
    "having":
        {
        "type": "not",
        "havingSpec":
            {
                "type": "equalTo",
                "aggregation": "<aggregate_metric>",
                "value": <numeric_value>
            }
        }
}
```
