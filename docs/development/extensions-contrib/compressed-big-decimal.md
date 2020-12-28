---
id: compressed-big-decimal
title: "Compressed Big Decimal"
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

## Overview
**Compressed Big Decimal** is an extension which provides support for Mutable big decimal value that can be used to accumulate values without losing precision or reallocating memory. This type helps in absolute precision arithmetic on large numbers in applications, where  greater level of accuracy is required, such as financial applications, currency based transactions.  This helps avoid rounding issues where in potentially large amount of money can be lost.

Accumulation requires that the two numbers have the same scale, but does not require that they are  of the same size. If the value being accumulated has a larger underlying array than this value (the result), then the higher order bits are dropped, similar to  what happens when adding a long to an int and storing the result in an int. A compressed big decimal that holds its data with an embedded array.

Compressed big decimal is an absolute number based complex type based on big decimal in Java. This supports all the functionalities supported by Java Big Decimal.  Java Big Decimal is not mutable in order to avoid big garbage collection issues.   Compressed big decimal is needed to mutate the value in the accumulator.

#### Main enhancements provided by this extension:
1. Functionality: Mutating Big decimal type with greater precision 
2. Accuracy: Provides greater level of accuracy in decimal arithmetic

## Operations
To use this extension, make sure to [load](../../development/extensions.md#loading-extensions) `compressed-big-decimal` to your config file.

## Configuration
There are currently no configuration properties specific to Compressed Big Decimal

## Limitations
* Compressed Big Decimal does not provide correct result when the value being accumulated has a larger underlying array than this value (the result), then the higher order bits are dropped, similar to  what happens when adding a long to an int and storing the result in an int.


### Ingestion Spec:
* Most properties in the Ingest spec derived from  [Ingestion Spec](../../ingestion/index.md) / [Data Formats](../../ingestion/data-formats.md)


|property|description|required?|
|--------|-----------|---------|
|metricsSpec|Metrics Specification, In metrics specification while specifying metrics details such as name, type should be specified as compressedBigDecimal|Yes|

### Query spec:
* Most properties in the query spec derived from  [groupBy query](../../querying/groupbyquery.md) / [timeseries](../../querying/timeseriesquery.md), see documentation for these query types.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be either "groupBy" OR "timeseries"; this is the first thing Druid looks at to figure out how to interpret the query.|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../../querying/datasource.md) for more information.|yes|
|dimensions|A JSON list of [DimensionSpec](../../querying/dimensionspecs.md) (Notice that property is optional)|no|
|limitSpec|See [LimitSpec](../../querying/limitspec.md)|no|
|having|See [Having](../../querying/having.md)|no|
|granularity|A period granularity; See [Period Granularities](../../querying/granularities.html#period-granularities)|yes|
|filter|See [Filters](../../querying/filters.md)|no|
|aggregations|Aggregations forms the input to Averagers; See [Aggregations](../../querying/aggregations.md). The Aggregations must specify type, scale and size as follows for compressedBigDecimal Type ```"aggregations": [{"type": "compressedBigDecimal","name": "..","fieldName": "..","scale": [Numeric],"size": [Numeric]}```.  Please refer query example in Examples section.  |Yes|
|postAggregations|Supports only aggregations as input; See [Post Aggregations](../../querying/post-aggregations.md)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

## Examples

Consider the data as

|Date|Item|SaleAmount|
|--------|-----------|---------|

```
20201208,ItemA,0.0
20201208,ItemB,10.000000000
20201208,ItemA,-1.000000000
20201208,ItemC,9999999999.000000000
20201208,ItemB,5000000000.000000005
20201208,ItemA,2.0
20201208,ItemD,0.0
```

IngestionSpec syntax:

```json
{
	"type": "index_parallel",
	"spec": {
		"dataSchema": {
			"dataSource": "invoices",
			"timestampSpec": {
				"column": "timestamp",
				"format": "yyyyMMdd"
			},
			"dimensionsSpec": {
				"dimensions": [{
					"type": "string",
					"name": "itemName"
				}]
			},
			"metricsSpec": [{
				"name": "saleAmount",
				"type": *"compressedBigDecimal"*,
				"fieldName": "saleAmount"
			}],
			"transformSpec": {
				"filter": null,
				"transforms": []
			},
			"granularitySpec": {
				"type": "uniform",
				"rollup": false,
				"segmentGranularity": "DAY",
				"queryGranularity": "none",
				"intervals": ["2020-12-08/2020-12-09"]
			}
		},
		"ioConfig": {
			"type": "index_parallel",
			"inputSource": {
				"type": "local",
				"baseDir": "/home/user/sales/data/staging/invoice-data",
				"filter": "invoice-001.20201208.txt"
			},
			"inputFormat": {
				"type": "tsv",
                                "delimiter": ",",
                                "skipHeaderRows": 0,
				"columns": [
						"timestamp",
						"itemName",
						"saleAmount"
					]
			}
		},
		"tuningConfig": {
			"type": "index_parallel"
		}
	}
}
```
### Group By Query  example

Calculating sales groupBy all.

Query syntax:

```json
{
    "queryType": "groupBy",
    "dataSource": "invoices",
    "granularity": "ALL",
    "dimensions": [
    ],
    "aggregations": [
        {
            "type": "compressedBigDecimal",
            "name": "saleAmount",
            "fieldName": "saleAmount",
            "scale": 9,
            "size": 3

        }
    ],
    "intervals": [
        "2020-01-08T00:00:00.000Z/P1D"
    ]
}
```

Result:

```json
[ {
  "version" : "v1",
  "timestamp" : "2020-12-08T00:00:00.000Z",
  "event" : {
    "revenue" : 15000000010.000000005
  }
} ]
```

Had you used *doubleSum* instead of compressedBigDecimal the result would be 

```json
[ {
  "timestamp" : "2020-12-08T00:00:00.000Z",
  "result" : {
    "revenue" : 1.500000001E10
  }
} ]
```
As shown above the precision is lost and could lead to loss in money.

### TimeSeries Query Example 

Query syntax:

```json
{
    "queryType": "timeseries",
    "dataSource": "invoices",
    "granularity": "ALL",
    "aggregations": [
        {
            "type": "compressedBigDecimal",
            "name": "revenue",
            "fieldName": "revenue",
            "scale": 9,
            "size": 3
        }
    ],
    "filter": {
        "type": "not",
        "field": {
            "type": "selector",
            "dimension": "itemName",
            "value": "ItemD"
        }
    },
    "intervals": [
        "2020-12-08T00:00:00.000Z/P1D"
    ]
}
```

Result:

```json
[ {
  "timestamp" : "2020-12-08T00:00:00.000Z",
  "result" : {
    "revenue" : 15000000010.000000005
  }
} ]
```
