---
id: test-stats
title: "Test stats aggregators"
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


The `druid-stats` extension for Apache Druid incorporates aggregators to compute test statistics, including z-scores and p-values.
Please refer to [Democratizing Experimentation Data for Product Innovations](https://medium.com/paypal-tech/democratizing-experimentation-data-for-product-innovations-8b6e1cf40c27) for math background and details.

Make sure to include `druid-stats` extension in order to use these aggregators.

## Z-Score for two sample ztests post aggregator

Please refer to [Making Sense of the Two-Proportions Test](https://www.isixsigma.com/tools-templates/hypothesis-testing/making-sense-two-proportions-test/) and [An Introduction to Statistics: Comparing Two Means](https://userweb.ucs.louisiana.edu/~jcb0773/Berry_statbook/427bookall-August2024.pdf) for more details.

```
z = (p1 - p2) / S.E.  (assuming null hypothesis is true)
```

Please see below for p1 and p2.
Please note S.E. stands for standard error where

```
S.E. = sqrt{ p1 * ( 1 - p1 )/n1 + p2 * (1 - p2)/n2) }
```
(p1 – p2) is the observed difference between two sample proportions.

### zscore2sample post aggregator

* **`zscore2sample`**: calculate the z-score using two-sample z-test while converting binary variables (***e.g.*** success or not) to continuous variables (***e.g.*** conversion rate).

```json
{
  "type": "zscore2sample",
  "name": "<output_name>",
  "successCount1": <post_aggregator> success count of sample 1,
  "sample1Size": <post_aggregaror> sample 1 size,
  "successCount2": <post_aggregator> success count of sample 2,
  "sample2Size" : <post_aggregator> sample 2 size
}
```

Please note the post aggregator will be converting binary variables to continuous variables for two population proportions.  Specifically

p1 = (successCount1) / (sample size 1)

p2 = (successCount2) / (sample size 2)

### pvalue2tailedZtest post aggregator

* **`pvalue2tailedZtest`**: calculate p-value of two-sided z-test from zscore
    - ***pvalue2tailedZtest(zscore)*** - the input is a z-score which can be calculated using the zscore2sample post aggregator


```json
{
  "type": "pvalue2tailedZtest",
  "name": "<output_name>",
  "zScore": <zscore post_aggregator>
}
```

## Example usage

In this example, we use zscore2sample post aggregator to calculate z-score, and then feed the z-score to pvalue2tailedZtest post aggregator to calculate p-value.

A JSON query example can be as follows:

```json
{
  ...
    "postAggregations" : {
    "type"   : "pvalue2tailedZtest",
    "name"   : "pvalue",
    "zScore" :
    {
     "type"   : "zscore2sample",
     "name"   : "zscore",
     "successCount1" :
       { "type"   : "constant",
         "name"   : "successCountFromPopulation1Sample",
         "value"  : 300
       },
     "sample1Size" :
       { "type"   : "constant",
         "name"   : "sampleSizeOfPopulation1",
         "value"  : 500
       },
     "successCount2":
       { "type"   : "constant",
         "name"   : "successCountFromPopulation2Sample",
         "value"  : 450
       },
     "sample2Size" :
       { "type"   : "constant",
         "name"   : "sampleSizeOfPopulation2",
         "value"  : 600
       }
     }
    }
}

```
