---
layout: doc_page
---
# Sorting Orders
These sorting orders are used by the [TopNMetricSpec](./topnmetricspec.html), [SearchQuery](./searchquery.html), GroupByQuery's [LimitSpec](./limitspec.html), and [BoundFilter](./filters.html#bound-filter).

## Lexicographic
Sorts values by converting Strings to their UTF-8 byte array representations and comparing lexicgraphically, byte-by-byte.

## Alphanumeric
Suitable for strings with both numeric and non-numeric content, e.g.: "file12 sorts after file2"

See https://github.com/amjjd/java-alphanum for more details on how this ordering sorts values.

This ordering is not suitable for numbers with decimal points or negative numbers. 
* For example, "1.3" precedes "1.15" in this ordering because "15" has more significant digits than "3". 
* Negative numbers are sorted after positive numbers (because numeric characters precede the "-" in the negative numbers).

## Numeric
Sorts values as numbers, supports integers and floating point values. Negative values are supported.

This sorting order will try to parse all string values as numbers. Unparseable values are treated as nulls, and nulls precede numbers.

When comparing two unparseable values (e.g., "hello" and "world"), this ordering will sort by comparing the unparsed strings lexicographically.

## Strlen
Sorts values by the their string lengths. When there is a tie, this comparator falls back to using the String compareTo method.
