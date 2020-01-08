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


This Apache Druid extension adds the ability to both construct bloom filters from query results, and filter query results by testing
against a bloom filter. Make sure to [include](../../development/extensions.md#loading-extensions) `druid-bloom-filter` as an
extension.

A Bloom filter is a probabilistic data structure for performing a set membership check. A bloom filter is a good candidate
to use with Druid for cases where an explicit filter is impossible, e.g. filtering a query against a set of millions of
 values.

Following are some characteristics of Bloom filters:

- Bloom filters are highly space efficient when compared to using a HashSet.
- Because of the probabilistic nature of bloom filters, false positive results are possible (element was not actually
inserted into a bloom filter during construction, but `test()` says true)
- False negatives are not possible (if element is present then `test()` will never say false).
- The false positive probability of this implementation is currently fixed at 5%, but increasing the number of entries
that the filter can hold can decrease this false positive rate in exchange for overall size.
- Bloom filters are sensitive to number of elements that will be inserted in the bloom filter. During the creation of bloom filter expected number of entries must be specified. If the number of insertions exceed
 the specified initial number of entries then false positive probability will increase accordingly.

This extension is currently based on `org.apache.hive.common.util.BloomKFilter` from `hive-storage-api`. Internally,
this implementation uses Murmur3 as the hash algorithm.

To construct a BloomKFilter externally with Java to use as a filter in a Druid query:

```java
BloomKFilter bloomFilter = new BloomKFilter(1500);
bloomFilter.addString("value 1");
bloomFilter.addString("value 2");
bloomFilter.addString("value 3");
ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
BloomKFilter.serialize(byteArrayOutputStream, bloomFilter);
String base64Serialized = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
```

This string can then be used in the native or SQL Druid query.

## Filtering queries with a Bloom Filter

### JSON Specification of Bloom Filter
```json
{
  "type" : "bloom",
  "dimension" : <dimension_name>,
  "bloomKFilter" : <serialized_bytes_for_BloomKFilter>,
  "extractionFn" : <extraction_fn>
}
```

|Property                 |Description                   |required?                           |
|-------------------------|------------------------------|----------------------------------|
|`type`                   |Filter Type. Should always be `bloom`|yes|
|`dimension`              |The dimension to filter over. | yes |
|`bloomKFilter`           |Base64 encoded Binary representation of `org.apache.hive.common.util.BloomKFilter`| yes |
|`extractionFn`|[Extraction function](../../querying/dimensionspecs.html#extraction-functions) to apply to the dimension values |no|


### Serialized Format for BloomKFilter

 Serialized BloomKFilter format:

 - 1 byte for the number of hash functions.
 - 1 big endian int(That is how OutputStream works) for the number of longs in the bitset
 - big endian longs in the BloomKFilter bitset

Note: `org.apache.hive.common.util.BloomKFilter` provides a serialize method which can be used to serialize bloom filters to outputStream.

### Filtering SQL Queries

Bloom filters can be used in SQL `WHERE` clauses via the `bloom_filter_test` operator:

```sql
SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(<expr>, '<serialized_bytes_for_BloomKFilter>')
```

### Expression and Virtual Column Support

The bloom filter extension also adds a bloom filter [Druid expression](../../misc/math-expr.md) which shares syntax
with the SQL operator.

```sql
bloom_filter_test(<expr>, '<serialized_bytes_for_BloomKFilter>')
```

## Bloom Filter Query Aggregator

Input for a `bloomKFilter` can also be created from a druid query with the `bloom` aggregator. Note that it is very
important to set a reasonable value for the `maxNumEntries` parameter, which is the maximum number of distinct entries
that the bloom filter can represent without increasing the false positive rate. It may be worth performing a query using
one of the unique count sketches to calculate the value for this parameter in order to build a bloom filter appropriate
for the query.

### JSON Specification of Bloom Filter Aggregator

```json
{
      "type": "bloom",
      "name": <output_field_name>,
      "maxNumEntries": <maximum_number_of_elements_for_BloomKFilter>
      "field": <dimension_spec>
    }
```

|Property                 |Description                   |required?                           |
|-------------------------|------------------------------|----------------------------------|
|`type`                   |Aggregator Type. Should always be `bloom`|yes|
|`name`                   |Output field name |yes|
|`field`                  |[DimensionSpec](../../querying/dimensionspecs.md) to add to `org.apache.hive.common.util.BloomKFilter` | yes |
|`maxNumEntries`          |Maximum number of distinct values supported by `org.apache.hive.common.util.BloomKFilter`, default `1500`| no |

### Example

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

response

```json
[{"timestamp":"2015-09-12T00:00:00.000Z","result":{"userBloom":"BAAAJhAAAA..."}}]
```

These values can then be set in the filter specification described above.

Ordering results by a bloom filter aggregator, for example in a TopN query, will perform a comparatively expensive
linear scan _of the filter itself_ to count the number of set bits as a means of approximating how many items have been
added to the set. As such, ordering by an alternate aggregation is recommended if possible.


### SQL Bloom Filter Aggregator
Bloom filters can be computed in SQL expressions with the `bloom_filter` aggregator:

```sql
SELECT BLOOM_FILTER(<expression>, <max number of entries>) FROM druid.foo WHERE dim2 = 'abc'
```

but requires the setting `druid.sql.planner.serializeComplexValues` to be set to `true`. Bloom filter results in a SQL
 response are serialized into a base64 string, which can then be used in subsequent queries as a filter.
