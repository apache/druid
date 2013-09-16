---
layout: default
---
Timeseries queries
==================

These types of queries take a timeseries query object and return an array of JSON objects where each object represents a value asked for by the timeseries query.

An example timeseries query object is shown below:

<pre>
<code>
{
 [queryType]() “timeseries”,
 [dataSource]() “sample\_datasource”,
 [granularity]() “day”,
 [filter]() {
 [type]() “and”,
 [fields]() [
 {
 [type]() “selector”,
 [dimension]() “sample\_dimension1”,
 [value]() “sample\_value1”
 },
 {
 [type]() “or”,
 [fields]() [
 {
 [type]() “selector”,
 [dimension]() “sample\_dimension2”,
 [value]() “sample\_value2”
 },
 {
 [type]() “selector”,
 [dimension]() “sample\_dimension3”,
 [value]() “sample\_value3”
 }
 ]
 }
 ]
 },
 [aggregations]() [
 {
 [type]() “longSum”,
 [name]() “sample\_name1”,
 [fieldName]() “sample\_fieldName1”
 },
 {
 [type]() “doubleSum”,
 [name]() “sample\_name2”,
 [fieldName]() “sample\_fieldName2”
 }
 ],
 [postAggregations]() [
 {
 [type]() “arithmetic”,
 [name]() “sample\_divide”,
 [fn]() “/”,
 [fields]() [
 {
 [type]() “fieldAccess”,
 [name]() “sample\_name1”,
 [fieldName]() “sample\_fieldName1”
 },
 {
 [type]() “fieldAccess”,
 [name]() “sample\_name2”,
 [fieldName]() “sample\_fieldName2”
 }
 ]
 }
 ],
 [intervals]() [
 “2012-01-01T00:00:00.000/2012-01-03T00:00:00.000”
 ]
}

</pre>
</code>

There are 7 main parts to a timeseries query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be “timeseries”; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|granularity|Defines the granularity of the query. See [[Granularities]]|yes|
|filter|See [[Filters]]|no|
|aggregations|See [[Aggregations]]|yes|
|postAggregations|See [[Post Aggregations]]|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

To pull it all together, the above query would return 2 data points, one for each day between 2012-01-01 and 2012-01-03, from the “sample\_datasource” table. Each data point would be the (long) sum of sample\_fieldName1, the (double) sum of sample\_fieldName2 and the (double) the result of sample\_fieldName1 divided by sample\_fieldName2 for the filter set. The output looks like this:

<pre>
<code>
[
 {
 [timestamp]() “2012-01-01T00:00:00.000Z”,
 [result]() {
 [sample\_name1]() <some_value>,
 [sample\_name2]() <some_value>,
 [sample\_divide]() <some_value>
 }
 },
 {
 [timestamp]() “2012-01-02T00:00:00.000Z”,
 [result]() {
 [sample\_name1]() <some_value>,
 [sample\_name2]() <some_value>,
 [sample\_divide]() <some_value>
 }
 }
]

</pre>
</code>
