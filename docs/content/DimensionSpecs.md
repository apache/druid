---
layout: doc_page
---

# Transforming Dimension Values

The following JSON fields can be used in a query to operate on dimension values.

## DimensionSpec

`DimensionSpec`s define how dimension values get transformed prior to aggregation.

### Default DimensionSpec

Returns dimension values as is and optionally renames the dimension.

```json
{ "type" : "default", "dimension" : <dimension>, "outputName": <output_name> }
```

### Extraction DimensionSpec

Returns dimension values transformed using the given [extraction function](#extraction-functions).

```json
{
  "type" : "extraction",
  "dimension" : <dimension>,
  "outputName" :  <output_name>,
  "extractionFn" : <extraction_function>
}
```

## Extraction Functions

Extraction functions define the transformation applied to each dimension value.

Transformations can be applied to both regular (string) dimensions, as well
as the special `__time` dimension, which represents the current time bucket
according to the query [aggregation granularity](Granularities.html).

**Note**: for functions taking string values (such as regular expressions),
`__time` dimension values will be formatted in [ISO-8601 format](https://en.wikipedia.org/wiki/ISO_8601)
before getting passed to the extraction function.

### Regular Expression Extraction Function

Returns the first matching group for the given regular expression.
If there is no match, it returns the dimension value as is.

```json
{ "type" : "regex", "expr" : <regular_expression> }
```

For example, using `"expr" : "(\\w\\w\\w).*"` will transform
`'Monday'`, `'Tuesday'`, `'Wednesday'` into `'Mon'`, `'Tue'`, `'Wed'`.

### Partial Extraction Function

Returns the dimension value unchanged if the regular expression matches, otherwise returns null.

```json
{ "type" : "partial", "expr" : <regular_expression> }
```

### Search Query Extraction Function

Returns the dimension value unchanged if the given [`SearchQuerySpec`](SearchQuerySpec.html)
matches, otherwise returns null.

```json
{ "type" : "searchQuery", "query" : <search_query_spec> }
```

### Time Format Extraction Function

Returns the dimension value formatted according to the given format string, time zone, and locale.

For `__time` dimension values, this formats the time value bucketed by the
[aggregation granularity](Granularities.html)

For a regular dimension, it assumes the string is formatted in
[ISO-8601 date and time format](https://en.wikipedia.org/wiki/ISO_8601).

* `format` : date time format for the resulting dimension value, in [Joda Time DateTimeFormat](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html).
* `locale` : locale (language and country) to use, given as a [IETF BCP 47 language tag](http://www.oracle.com/technetwork/java/javase/java8locales-2095355.html#util-text), e.g. `en-US`, `en-GB`, `fr-FR`, `fr-CA`, etc.
* `timeZone` : time zone to use in [IANA tz database format](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones), e.g. `Europe/Berlin` (this can possibly be different than the aggregation time-zone)

```json
{ "type" : "timeFormat",
  "format" : <output_format>,
  "timeZone" : <time_zone> (optional),
  "locale" : <locale> (optional) }
```

For example, the following dimension spec returns the day of the week for Montréal in French:

```json
{
  "type" : "extraction",
  "dimension" : "__time",
  "outputName" :  "dayOfWeek",
  "extractionFn" : {
    "type" : "timeFormat",
    "format" : "EEEE",
    "timeZone" : "America/Montreal",
    "locale" : "fr"
  }
}
```

### Time Parsing Extraction Function

Parses dimension values as timestamps using the given input format,
and returns them formatted using the given output format.

Note, if you are working with the `__time` dimension, you should consider using the
[time extraction function instead](#time-format-extraction-function) instead,
which works on time value directly as opposed to string values.

Time formats are described in the
[SimpleDateFormat documentation](http://icu-project.org/apiref/icu4j/com/ibm/icu/text/SimpleDateFormat.html)

```json
{ "type" : "time",
  "timeFormat" : <input_format>,
  "resultFormat" : <output_format> }
```


### Javascript Extraction Function

Returns the dimension value, as transformed by the given JavaScript function.

For regular dimensions, the input value is passed as a string.

For the `__time` dimension, the input value is passed as a number
representing the number of milliseconds since January 1, 1970 UTC.

Example for a regular dimension

```json
{
  "type" : "javascript",
  "function" : "function(str) { return str.substr(0, 3); }"
}
```

Example for the `__time` dimension:

```json
{
  "type" : "javascript",
  "function" : "function(t) { return 'Second ' + Math.floor((t % 60000) / 1000); }"
}
```

### Namespaced extraction function (EXPERIMENTAL)
A namespaced extraction function is a simple key-value mapping where the key-value mappings are unique to a particular namespace.
This can be used for re-naming values in a datasource for certain queries. To use a particular namespace, simply set the appropriate namespace for the dimExtractionFn
```json
    "dimExtractionFn" : {
      "type":"namespace",
      "namespace":"some_namespace"
    }
```
Namespace updates can be set through adding a child in json form to the zookeeper path at`druid.zk.paths.namespacePath`. The json is of the following format:

#### URI namespace update
The remapping values for each namespace can be specified by a namespace update json object as per
```json
{
  "namespace":{
    "type":"uri",
    "namespace":"some_namespace",
    "uri": "file:///some/file/or/other/uri",
    "parseSpec":{
      "type":"csv",
      "columns":["key","value]
    }
  },
  "updateMs":0
}
```
The `updateMs` value specifies the period in milliseconds between checks for updates. If the source of the namespace is capable of providing a timestamp, the namespace will only be updated if it has changed since the prior tick of `updateMs`. A value of 0 means populate once and do not attempt to update.

The parseSpec can be one of a number of values. Each of the examples below would rename foo to bar, baz to bat, and buck to truck. All parseSpec types assumes each input is delimited by a new line.

##### csv ParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|

*example input*
```
bar,something,foo
bat,something2,baz
truck,something3,buck
```

*example parseSpec*
```json
"parseSpec": {
  "type": "csv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value"
}
```

##### tsv ParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|
|`delimiter`|The delimiter in the file|no|tab (`\t`)|


*example input*
```
bar|something,1|foo
bat|something,2|baz
truck|something,3|buck
```

*example parseSpec*
```json
"parseSpec": {
  "type": "tsv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value",
  "delimiter": "|"
}
```

##### customJson ParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`keyFieldName`|The field name of the key|yes|null|
|`valueFieldName`|The field name of the value|yes|null|

*example input*
```json
{"key": "foo", "value": "bar", "somethingElse" : "something"}
{"key": "baz", "value": "bat", "somethingElse" : "something"}
{"key": "buck", "somethingElse": "something", "value": "truck"}
```

*example parseSpec*
```json
"parseSpec": {
  "type": "customJson",
  "keyFieldName": "key",
  "valueFieldName": "value"
}
```


##### simpleJson ParseSpec
The `simpleJson` parseSpec does not take any parameters. It is simply a line delimited json file where the field is the key, and the field's value is the value.

*example input*
 
```json
{"foo": "bar"}
{"baz": "bat"}
{"buck": "truck"}
```

*example parseSpec*
```json
"parseSpec":{
  "type": "simpleJson"
}
```


#### JDBC namespace update
The JDBC namespaces will pull from a database cache. If the `tsColumn` is set it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. For example, the following must be valid sql for the table `SELECT * FROM some_namespace_table WHERE timestamp_column >  '2015-01-01 00:00:00'`. If `tsColumn` is set, the caching service will attempt to only pull values that were written *after* the last sync. If `tsColumn` is not set, the entire table is pulled every time.

```json
{
  "namespace":{
    "namespace":"some_namespace",
    "connectorConfig":{
      "createTables":true,
      "connectURI":"jdbc:mysql://localhost:3306/druid",
      "user":"druid",
      "password":"diurd"
    },
    "table":"some_namespace_table",
    "keyColumn":"the_old_dim_value",
    "valueColumn":"the_new_dim_value",
    "tsColumn":"timestamp_column"
  },
  "updateMs":600000
}
```

### Kafka namespace update (Experimental)
It is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8). This requires the following extension: "io.druid.extensions:druid-dim-rename-kafka8"
```json
{
  "namespace":{
    "type":"kafka",
    "namespace":"testTopic",
    "kafkaTopic":"testTopic"
  }
}
```
#### Kafka renames
The extension `druid-dim-rename-kafka8` enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

Currently the historical node caches the key/value pairs from the kafka feed in an ephemeral memory mapped DB via MapDB.

Current limitations:
* All rename feeds must be known at server startup and are not dynamically configurable
* The query issuer must know the name of the kafka topic

#### Configuration
The following options are used to define the behavior:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.rename.kafka.properties`|A json map of kafka consumer properties. See below for special properties.|See below|

The following are the handling for kafka consumer properties in `druid.query.rename.kafka.properties`

|Property|Description|Default|
|--------|-----------|-------|
|`zookeeper.connect`|Zookeeper connection string|`localhost:2181/kafka`|
|`group.id`|Group ID, auto-assigned for publish-subscribe model and cannot be overridden|`UUID.randomUUID().toString()`|
|`auto.offset.reset`|Setting to get the entire kafka rename stream. Cannot be overridden|`smallest`|

#### Hooking up namespaces
To hook up a kafka topic to a namespace, a `KafkaExtractionNamespace` used in the namespace update posted to zookeeper.

#### Testing the kafka rename functionality
To test this setup, you can send key/value pairs to a kafka stream via the following producer console
`./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic`
Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter)