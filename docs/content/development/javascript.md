---
layout: doc_page
---
# JavaScript Programming Guide

This page discusses how to use JavaScript to extend Druid.

## Examples

JavaScript can be used to extend Druid in a variety of ways:

- [Aggregators](../querying/aggregations.html#javascript-aggregator)
- [Extraction functions](../querying/dimensionspecs.html#javascript-extraction-function)
- [Filters](../querying/filters.html#javascript-filter)
- [Post-aggregators](../querying/post-aggregations.html#javascript-post-aggregator)
- [Input parsers](../ingestion/data-formats.html#javascript)
- [Router strategy](../development/router.html#javascript)
- [Worker select strategy](../configuration/indexing-service.html#javascript)

JavaScript can be injected dynamically at runtime, making it convenient to rapidly prototype new functionality
without needing to write and deploy Druid extensions.

Druid uses the Mozilla Rhino engine at optimization level 9 to compile and execute JavaScript.

## Security

Druid does not execute JavaScript functions in a sandbox, so they have full access to the machine. So Javascript
functions allow users to execute arbitrary code inside druid process. So, by default, Javascript is disabled.
However, on dev/staging environments or secured production environments you can enable those by setting
the [configuration property](../configuration/index.html)
`druid.javascript.enabled = true`.

## Global variables

Avoid using global variables. Druid may share the global scope between multiple threads, which can lead to
unpredictable results if global variables are used.

## Performance

Simple JavaScript functions typically have a slight performance penalty to native speed. More complex JavaScript
functions can have steeper performance penalties. Druid compiles JavaScript functions once per node per query.

You may need to pay special attention to garbage collection when making heavy use of JavaScript functions, especially
garbage collection of the compiled classes themselves. Be sure to use a garbage collector configuration that supports
timely collection of unused classes (this is generally easier on JDK8 with the Metaspace than it is on JDK7).

## JavaScript vs. Native Extensions

Generally we recommend using JavaScript when security is not an issue, and when speed of development is more important
than performance or memory use. If security is an issue, or if performance and memory use are of the utmost importance,
we recommend developing a native Druid extension.

In addition, native Druid extensions are more flexible than JavaScript functions. There are some kinds of extensions
(like sketches) that must be written as native Druid extensions due to their need for custom data formats.
