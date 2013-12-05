---
layout: doc_page
---

## DimensionSpec

`DimensionSpec`s define how dimension values get transformed prior to aggregation.

### DefaultDimensionSpec

Returns dimension values as is and optionally renames renames the dimension.

```json
{ "type" : "default", "dimension" : <dimension>, "outputName": <output_name> }
```

### ExtractionDimensionSpec

Returns dimension values transformed using the given [DimExtractionFn](#toc_3)

```json
{
  "type" : "extraction",
  "dimension" : <dimension>,
  "outputName" :  <output_name>,
  "dimExtractionFn" : <dim_extraction_fn>
}
```

## DimExtractionFn

`DimExtractionFn`s define the transformation applied to each dimenion value

### RegexDimExtractionFn

Returns the first group matched by the given regular expression. If there is no match it returns the dimension value as is.

```json
{ "type" : "regex", "expr", <regular_expression> }
```

### PartialDimExtractionFn

Returns the dimension value as is if there is a match, otherwise returns null.

```json
{ "type" : "partial", "expr", <regular_expression> }
```

### SearchQuerySpecDimExtractionFn

Returns the dimension value as is if the given [SearchQuerySpec](SearchQuerySpec.html) matches, otherwise returns null.

```json
{ "type" : "searchQuery", "query" : <search_query_spec> }
```

### TimeDimExtractionFn

Parses dimension values as timestamps using the given input format, and returns them formatted using the given output format. Time formats follow the [com.ibm.icu.text.SimpleDateFormat](http://icu-project.org/apiref/icu4j/com/ibm/icu/text/SimpleDateFormat.html) format

```json
{ "type" : "time", "timeFormat" : <input_format>, "resultFormat" : <output_format> }
```

### JavascriptDimExtractionFn

Returns the dimension value as transformed by the given JavaScript function.

Example

```json
{
  "type" : "javascript",
  "function" : "function(str) { return str.substr(0, 3); }"
}
