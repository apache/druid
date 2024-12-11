---
id: bloom-filter
title: "Bloom Filter"
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


To use the Apache Druid&circledR; Bloom filter extension, include `druid-bloom-filter` in the extensions load list. See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.

This extension adds the abilities to construct Bloom filters from query results and to filter query results by testing
against a Bloom filter. A Bloom filter is a probabilistic data structure to check for set membership. A Bloom
filter is a good candidate to use when an explicit filter is impossible, such as filtering a query
against a set of millions of values.

Following are some characteristics of Bloom filters:

- Bloom filters are significantly more space efficient than HashSets.
- Because they are probabilistic, false positive results are possible with Bloom filters. For example, the `test()` function might return `true` for an element that is not within the filter.
- False negatives are not possible. If an element is present, `test()` always returns `true`.
- The false positive probability of this implementation is fixed at 5%. Increasing the number of entries that the filter can hold can decrease this false positive rate in exchange for overall size.
- Bloom filters are sensitive to the number of inserted elements. You must specify the expected number of entries at creation time. If the number of insertions exceeds the specified number of entries, the false positive probability increases accordingly.

This extension is based on `org.apache.hive.common.util.BloomKFilter` from `hive-storage-api`. Internally,
this implementation uses Murmur3 as the hash algorithm.

The following Java example shows how to construct a BloomKFilter externally:

```java
BloomKFilter bloomFilter = new BloomKFilter(1500);
bloomFilter.addString("value 1");
bloomFilter.addString("value 2");
bloomFilter.addString("value 3");
ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
BloomKFilter.serialize(byteArrayOutputStream, bloomFilter);
String base64Serialized = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
```

You can then use the Base64 encoded string in JSON-based or SQL-based queries in Druid.

## Filter queries with a Bloom filter

### JSON specification

```json
{
  "type" : "bloom",
  "dimension" : <dimension_name>,
  "bloomKFilter" : <serialized_bytes_for_BloomKFilter>,
  "extractionFn" : <extraction_fn>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`type`|Filter type. Set to `bloom`.|Yes|
|`dimension`|Dimension to filter over.|Yes|
|`bloomKFilter`|Base64 encoded binary representation of `org.apache.hive.common.util.BloomKFilter`.|Yes|
|`extractionFn`|[Extraction function](../../querying/dimensionspecs.md#extraction-functions) to apply to the dimension values.|No|

### Serialized format for BloomKFilter

Serialized BloomKFilter format:

- 1 byte for the number of hash functions.
- 1 big-endian integer for the number of longs in the bitset.
- Big-endian longs in the BloomKFilter bitset.

`org.apache.hive.common.util.BloomKFilter` provides a method to serialize Bloom filters to `outputStream`.

### Filter SQL queries

You can use Bloom filters in SQL `WHERE` clauses with the `bloom_filter_test` operator:

```sql
SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(<expr>, '<serialized_bytes_for_BloomKFilter>')
```

### Expression and virtual column support

The Bloom filter extension also adds a Bloom filter [Druid expression](../../querying/math-expr.md) which shares syntax
with the SQL operator.

```sql
bloom_filter_test(<expr>, '<serialized_bytes_for_BloomKFilter>')
```

## Bloom filter query aggregator

You can create an input for a `BloomKFilter` from a Druid query with the `bloom` aggregator. Make sure to set a reasonable value for the `maxNumEntries` parameter to specify the maximum number of distinct entries that the Bloom filter can represent without increasing the false positive rate. Try performing a query using
one of the unique count sketches to calculate the value for this parameter to build a Bloom filter appropriate for the query.

### JSON specification

```json
{
      "type": "bloom",
      "name": <output_field_name>,
      "maxNumEntries": <maximum_number_of_elements_for_BloomKFilter>
      "field": <dimension_spec>
    }
```

|Property|Description|Required|
|--------|-----------|--------|
|`type`|Aggregator type. Set to `bloom`.|Yes|
|`name`|Output field name.|Yes|
|`field`|[DimensionSpec](../../querying/dimensionspecs.md) to add to `org.apache.hive.common.util.BloomKFilter`.|Yes|
|`maxNumEntries`|Maximum number of distinct values supported by `org.apache.hive.common.util.BloomKFilter`. Defaults to `1500`.|No|

### Example

The following example shows a timeseries query object with a `bloom` aggregator:

```json
{
  "queryType": "timeseries",
  "dataSource": "wikiticker",
  "intervals": [ "2015-09-12T00:00:00.000/2015-09-13T00:00:00.000" ],
  "granularity": "day",
  "aggregations": [
    {
      "type": "bloom",
      "name": "userBloom",
      "maxNumEntries": 100000,
      "field": {
        "type":"default",
        "dimension":"user",
        "outputType": "STRING"
      }
    }
  ]
}
```

Example response:

```json
[
  {
    "timestamp":"2015-09-12T00:00:00.000Z",
    "result":{"userBloom":"BAAAJhAAAA..."}
  }
]
```

We recommend ordering by an alternative aggregation method instead of ordering results by a Bloom filter aggregator.
Ordering results by a Bloom filter aggregator can be resource-intensive because Druid performs an expensive linear scan of the filter to approximate the count of items added to the set by counting the number of set bits. 

### SQL Bloom filter aggregator

You can compute Bloom filters in SQL expressions with the BLOOM_FILTER aggregator. For example:

```sql
SELECT BLOOM_FILTER(<expression>, <max number of entries>) FROM druid.foo WHERE dim2 = 'abc'
```

Druid serializes Bloom filter results in a SQL response into a Base64 string. You can use the resulting string in subsequent queries as a filter.
