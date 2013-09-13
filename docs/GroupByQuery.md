These types of queries take a groupBy query object and return an array of JSON objects where each object represents a grouping asked for by the query.

An example groupBy query object is shown below:

<pre>
<code>
{
 [queryType]() “groupBy”,
 [dataSource]() “sample\_datasource”,
 [granularity]() “day”,
 [dimensions]() [“dim1”, “dim2”],
 [limitSpec]() {
 [type]() “default”,
 [limit]() 5000,
 [columns]() [“dim1”, “metric1”]
 },
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
 ],
 [having]() {
 [type]() “greaterThan”,
 [aggregation]() “sample\_name1”,
 [value]() 0
 }
}

</pre>
</code>

There are 9 main parts to a groupBy query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be “groupBy”; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|dimensions|A JSON list of dimensions to do the groupBy over|yes|
|orderBy|See [[OrderBy]].|no|
|having|See [[Having]].|no|
|granularity|Defines the granularity of the query. See [[Granularities]]|yes|
|filter|See [[Filters]]|no|
|aggregations|See [[Aggregations]]|yes|
|postAggregations|See [[Post Aggregations]]|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

To pull it all together, the above query would return *n\*m* data points, up to a maximum of 5000 points, where n is the cardinality of the “dim1” dimension, m is the cardinality of the “dim2” dimension, each day between 2012-01-01 and 2012-01-03, from the “sample\_datasource” table. Each data point contains the (long) sum of sample\_fieldName1 if the value of the data point is greater than 0, the (double) sum of sample\_fieldName2 and the (double) the result of sample\_fieldName1 divided by sample\_fieldName2 for the filter set for a particular grouping of “dim1” and “dim2”. The output looks like this:

<pre>
<code>
[ {
 “version” : “v1”,
 “timestamp” : “2012-01-01T00:00:00.000Z”,
 “event” : {
 “dim1” : <some_dim1_value>,
 “dim2” : <some_dim2_value>,
 “sample\_name1” : <some_sample_name1_value>,
 “sample\_name2” :<some_sample_name2_value>,
 “sample\_divide” : <some_sample_divide_value>
 }
}, {
 “version” : “v1”,
 “timestamp” : “2012-01-01T00:00:00.000Z”,
 “event” : {
 “dim1” : <some_other_dim1_value>,
 “dim2” : <some_other_dim2_value>,
 “sample\_name1” : <some_other_sample_name1_value>,
 “sample\_name2” :<some_other_sample_name2_value>,
 “sample\_divide” : <some_other_sample_divide_value>
 }
},
…
]

</pre>
</code>
