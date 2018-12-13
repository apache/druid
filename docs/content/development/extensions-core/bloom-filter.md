---
layout: doc_page
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

# Bloom Filter

Make sure to [include](../../operations/including-extensions.html) `druid-bloom-filter` as an extension.

BloomFilter is a probabilistic data structure for set membership check.
Following are some characterstics of BloomFilter
- BloomFilters are highly space efficient when compared to using a HashSet.
- Because of the probabilistic nature of bloom filter false positive (element not present in bloom filter but test() says true) are possible
- false negatives are not possible (if element is present then test() will never say false).
- The false positive probability is configurable (default: 5%) depending on which storage requirement may increase or decrease.
- Lower the false positive probability greater is the space requirement.
- Bloom filters are sensitive to number of elements that will be inserted in the bloom filter.
- During the creation of bloom filter expected number of entries must be specified.If the number of insertions exceed the specified initial number of entries then false positive probability will increase accordingly.

Internally, this implementation of bloom filter uses Murmur3 fast non-cryptographic hash algorithm.

### JSON Representation of Bloom Filter

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
|`extractionFn`|[Extraction function](./../dimensionspecs.html#extraction-functions) to apply to the dimension values |no|


### Serialized Format for BloomKFilter
 Serialized BloomKFilter format:
 - 1 byte for the number of hash functions.
 - 1 big endian int(That is how OutputStream works) for the number of longs in the bitset
 - big endian longs in the BloomKFilter bitset

Note: `org.apache.hive.common.util.BloomKFilter` provides a serialize method which can be used to serialize bloom filters to outputStream.
