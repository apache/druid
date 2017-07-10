---
layout: doc_page
---

# Test Stats Aggregators

Incorporates test statistics related aggregators, including z-score and p-value. Please refer to [https://www.paypal-engineering.com/2017/06/29/democratizing-experimentation-data-for-product-innovations/](https://www.paypal-engineering.com/2017/06/29/democratizing-experimentation-data-for-product-innovations/) for background and details.

Make sure to include `druid-stats` extension in order to use these aggregrators.

## Z-Score for two sample ztests post aggregator

Please refer to [https://www.isixsigma.com/tools-templates/hypothesis-testing/making-sense-two-proportions-test/](https://www.isixsigma.com/tools-templates/hypothesis-testing/making-sense-two-proportions-test/) and [http://www.ucs.louisiana.edu/~jcb0773/Berry_statbook/Berry_statbook_chpt6.pdf](http://www.ucs.louisiana.edu/~jcb0773/Berry_statbook/Berry_statbook_chpt6.pdf) for more details.

z = (p1 - p2) / S.E.  (assuming null hypothesis is true)

where S.E. stands for standard error, and

S.E. = sqrt{ p1 * ( 1 - p1 )/n1 + p2 * (1 - p2)/n2) }

(p1 – p2) is the observed difference between two sample proportions.

### zscore2sample post aggregator
* **`zscore2sample`**: calculate the z-score using two-sample z-test while converting binary variables (***e.g.*** success or not) to continuous variables (***e.g.*** conversion rate).

```json
{
  "type": "zscore2sample",
  "name": "<output_name>",
  "fields": [<count 1 (post_aggregator1)>, <sample size 1 (post_aggregator2)>, <count 2 (post_aggregator3)>, <sample size 2 (post_aggregator4)>]
}
```
Please note as the post aggregator will be converting binary variables to continuous variables for two population proportions, it is sensitive to the ordering of the post aggregators.  In other words,

p1 = (count 1) / (sample size 1)

p2 = (count 2) / (sample size 2)

For example,

```
"fields": [<total success count of population 1>, <sample size of population 1>, <total success count of population 2>, <sample size of population 2>]
```

### pvalue2tailedztest post aggregator

* **`pvalue2tailedztest`**: calculate p-value for two sided z-test from zscore
    - ***pvalue2tailedZtest(zscore)*** - the input is the z-score calculated using zscore2samples post aggregator


```json
{
  "type": "pvalue2tailedztest",
  "name": "<output_name>",
  "field": "<aggregator_name>"
}
```

For example,

```
  "type": "pvalue2tailedztest",
  "name": "pvalue",
  "field": <zscore>
```
  
## Example Usage

In this example, we use zscore2sample post aggregator to calculate z-score, and feed the z-score to pvalue2tailedztest post aggregator to calculate p-value.

A JSON query example can be as follows:

```json
{
  ...
    "postAggregations" : {
    "type"   : "pvalue2tailedztest",
    "name"   : "pvalue",
    "field" : 
    {
     "type"   : "zscore2sample",
     "name"   : "zscore",
     "fields" : [
       { "type"   : "constant",
         "name"   : "successCountPopulation1",
         "value"  : 300
       },
       { "type"   : "constant",
         "name"   : "sampleSizePopulation1",
         "value"  : 500
       },
       { "type"   : "constant",
         "name"   : "successCountPopulation2",
         "value"  : 450
       },
       { "type"   : "constant",
         "name"   : "sampleSizePopulation2",
         "value"  : 600
       }
     ]
     }
    }
}

```
