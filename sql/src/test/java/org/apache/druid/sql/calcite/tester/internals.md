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

# Planner Test Internals

This page explains how the test framework does its job. You don't havt to know
this information to use the framework, but it certainly helps as the test
setup is complex and somewhat fragile as it depends on mocks and special-case
code: you may have to know how to fix or extend it.

This page also provides a reference for the case file structure.

## Introduction

Druid is a bit of a black box: queries go in and results come out:

```text
  +-------+     +-------+     +---------+
  | query | --> | Druid | --> | results |
  +-------+     +-------+     +---------+
    SQL                        JSON, CSV
```

While this is the perfect view for users (who, after all, just want to
query data), it is somewhat awkward for testing: the Druid black box is a
bit of a "then a miracle occurs" element from a testing perspective.

Internally, Druid actually has two main stages: plan and run:

```text
  +-------+     +---------+     +------+     +--------+     +---------+
  | query | --> | Planner | --> | Plan | --> | Engine | --> | results |
  +-------+     +---------+     +------+     +--------+     +---------+
    SQL            Druid        Calcite       Broker +       JSON, CSV
                 Planner +       Plan +      Historical
                  Calcite      Native Query
```

In fact, the process is even more complex as there are up to three
parts to the planning process:

* Calcite: parse SQL, produce a logical plan, and optimize.
* Druid native query
* "Physical plan" sent to the execution engine (mostly for `INSERT`).

We observe that the plan (in Druid's case, the native query) is the
API between the planner and the engine. The planner' job is to translate
SQL to a native query (by way of a Calcite logical plan), and the job of
the Broker + Historical "engine" is to faithfully execute the native
query it is given. As a result, we can test the planner and engine
separately.

The same is true for `INSERT` queries, though since implementation is
an extension, the actual checking of the corresponding physical plan
must also be done via an extension.

The goal of this package is to provide detailed, repeatable testing of
the planner, while making the planner's artifacts easy to visualize and
inspect. Visualization is important: the planner is complex and the best
way to understand (and test) it is by looking at its artifacts. These
include:

* The Calcite logical plan.
* The output schema.
* The Druid native query.

We assert that the planner works (a change did not break anything) if
the same SQL as input produces the same planner outputs both before and
after the change. Or, in other cases, that the *only* change in outputs
is the one we intended to produce: there are no accidental side effects.

### Test Flow

To test, we create a test case that defines what to test:

* The SQL query
* Planner settings (if any)
* Query context
* Parameter values (if any)

Then, we define what we expect the plannner to produce:

* The Calcite logical plan.
* The output schema.
* The Druid native query.
* Expected errors (if the query should fail.)

We then define a JUnit test which sets up the planner with whatever
configuration we require (such as a set of views, sample inputs, etc.)
The test flow is then:

```text
  +-----------+     +------------+     +-----------+
  | Test Case | --> | JUnit Test | --> | Pass/Fail |
  +-----------+     +------------+     +-----------+
```

See `v2.DruidPlannerTest.java` for the test case. See
`calcite/cases/*.case` for the test case inputs.

## Case File Structure

The case file consists of comments and zero or more cases. The file has a
rather unusual syntax: lines that start with three equal signs (`===`)
indicate sections. Why the odd syntax? Test cases include JSON, SQL
comments and CSV results. We need a syntax that is very unlikly to collide
with these various contents. Example case structure:

```text
=== case
I'm a test case
=== sql
SELECT * FROM myTable
```

The case file starts with comments which is handy way to include a
copyright notice: everything up to the first section boundary is a
comment.

There are two kinds of sections: contents and comments. Content sections
have names which are case in-sensitive. For example, both `sql` and `SQL`
are fine. Everything from the section head to the next section head is the
body of that one section.

Content sections themselves are of two kinds, though their syntax is
identical:

* Inputs to the planner
* Expected outputs from the planner

All sections (aside from `case` and `sql`) are optional. Provide the input
sections only if the input is needed, provide the output sections only for
those items to be verified. A test case with no expected output sections
will run and assert that the query does not throw an exception.

Sections can be copied from the previous tests (see below.) Many output
sections support regular expressions (see below.)

### Comments

A comment section is any section that starts with four or more
equal signs. This is a handy way to separate tests and provide comments
about the test:

```text
Test cases from the CalciteArraysQueryTest file

==============================================================
Converted from testSelectConstantArrayExpressionFromTable()

Verifies the array literal syntax.
=== case
SELECT constant array expression from table
=== SQL
SELECT ARRAY[1,2] as arr, dim1 FROM foo LIMIT 1
```

By convention, the comments that immediately preceed a test
case are assumed to describe that test case. Here the preceeding
comments are the "Converted from..." lines, but not the
"Test cases from..." lines.

When a test is converted from an existing Java-based test, reference
the test function as shown above. Otherwise, explain the purpose
of the test, or explain any unusual characteristics. (See the
existing cases for examples.)

### `case` Section

The `case` section must be the first one in each test case: it
announces the start of a new test. Everything in the case section
is the test label: it will appear in the logs if an error is found
for the test. This works best if the label is a single line. If
converting from an existing Java test, just convert the method
name to words.

### `SQL` Section

The SQL section contains the query for the test and is required. (It is hard
to test the planner without a SQL statement.) Please format the SQL statement
nicely as it can contain newlines:

```text
=== SQL
SELECT
  ARRAY[1,2] as arr,
  dim1
FROM foo
LIMIT 1
```

### `context` Section

The `context` section provides the query context, in "Java properties" format:
that is, as `name=value` pairs:

```text
=== context
maxSubqueryRows=2
```

The tests use metadata to determine the context variable type. In the above,
metdata tells
us that `maxSubqueryRows` is an `int`, so the value is converted to an `int`
internally. The type is assumed to be `String` if there is no metadata. If you
add a query context value, or use one not in the `QueryContexts` metadata table,
you may encounter an error if the test case loader guess the type wrong. To fix
the issue, add your parameter to the `QueryContexts` metadata table.

As a result, you can choose to quote strings or not. You must quote
strings if they start or end with spaces:

```
=== context
example=" quote me! "
```

### `parameters` Section

Druid supports query parameters. The `parameters` section provides the
parameter values to use when planning the query. Parameter values are typed,
so you provide values using a `<type>: <value>` syntax:

```text
=== sql
SELECT
  foo,
  bar
  FROM myTable
  WHERE foo = ?
    AND bar < ?
=== parameters
varchar: "a"
integer: 1
```

Use SQL types: `varchar` for `string`, `bigint` for `long`, etc.
`int` is accepted as a shorthand for `integer`.
As a convenience the code also accepts the Druid types (`string`, `long`).
But, since this is SQL, it is better to use the SQL types.

Names are case-insensitive:

```text
=== parameters
VARCHAR: "a"
INT: 1
```

SQL requires that there be one parameter value for each parameter in the
query, listed in the order that the parameters appear textual in the query.

Quoting of strings is optional. If unquoted, leading and trailing whitespace
is removed. Use quotes if you want to include such whitespace:

```text
=== parameters
VARCHAR: "  quote me!  "
INT: 1
```

### `options` Section

The `options` section provides instructions for setting up the planner or for
running the test. Options specify things which would otherwise be specified in code
or in the various `.properties` files. The name are mostly specific to this test
framework.

```text
=== options
failure=run
replacewithDefault=true
```

Supported option names include:

* `planner.<option>` - `PlannerConfig` options including:
  * `maxTopNLimit`
  * `useApproximateCountDistinct`
  * `useApproximateTopN`
  * `requireTimeCondition`
  * `useGroupingSetForExactDistinct`
  * `computeInnerJoinCostAsFilter`
  * `useNativeQueryExplain`
  * `maxNumericInFilters`
  * `sqlTimeZone`
* `unicodeEscapes`: either `true` or `false` (default). Handles the
  obscure case in which the SQL query contains a Java-style Unicode
  escape sequence. If `true` the test framework will replace such sequences
  with the equivalent Unicode character. Currently used by only a single
  test case.

We assume you know the meaning of the `PlannerConfig` options. If not, see
`PlannerConfig` for details. Types of options are inferred by the code using
query context-style parsing of strings to the right type. If you find you need
another option not on the above list, see `QueryTestCases` for how to add
more options.

Runtime options:

* `sqlCompatibleNulls` - `true`, `false` or `both`. Controls whether
  the test case (or run) should be used for "clasic" or SQL-compatible
  null handling. (`sqlCompatibleNulls=false` is the same as the internal
  option `replacewithDefault=true`.) (Maps to
  `NullHandling.initializeForTests()`.)
* `vectorize` - `true` (run with and without vectorization) or `false`
  (run only without vectorization.) (Adapted from `BaseCalciteTest`.)
* `allowNestedArrays`: either `true` or `false` (default). Enables nested
  array handling via `ExpressionProcessing.initializeForTests(true)`.
  (Though this is a global setting, the framework restores the original
  value after the test that changes this property.)
* `typedCompare` - `true` to parse the expected results into Java objects,
  then do an approximate comparison on floating-point values. `false`
  (the default) to do a text comparison.

### `ast` Section

Indicates the expected abstract syntax tree (AST) from the Calcite SQL
parser.

```text
== SQL explain
EXPLAIN PLAN FOR SELECT 1
== ast
EXPLAIN
  SELECT
    SELECT: (
      LITERAL - NumericLiteral (1)
    )
  LITERAL (EXPPLAN_ATTRIBUTES)
  LITERAL (PHYSICAL)
  LITERAL (TEXT)
```

### `unparsed` Section

Indicates the SQL text obtained by "unparsing" the AST.

```text
== SQL explain
EXPLAIN PLAN FOR SELECT 1
=== unparsed
EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR
SELECT 1
```

### `schema` Section

Indicates the expected result schema from the planner:

```text
=== SQL
SELECT dim1, COUNT(*)
FROM druid.foo
GROUP BY dim1
ORDER BY dim1 DESC
=== schema
dim1 VARCHAR
EXPR$1 BIGINT
```

Format is SQL-like: column name and SQL types, one column per line, with no commas.
Use upper-case types as the results are compared textually, and Calcite reports types
in upper case.

### `targetSchema` Section

Indicates the expected schema of the output table for an `INSERT` query. This is
different than `schema`, because an `INSERT` query could include a schema that
reports, say, the number of rows inserted.

```text
=== SQL
INSERT INTO druid.dst
SELECT
    __time,
    FLOOR(m1) as floor_m1,
    dim1 FROM foo
PARTITIONED BY DAY
CLUSTERED BY 2, dim1
=== schema
dataSource VARCHAR
signature OTHER
=== targetSchema
__time TIMESTAMP(3)
floor_m1 FLOAT
dim1 VARCHAR
```

### `plan` Section

Indicates the expected Calcite logical plan. For the above query:

```text
=== plan
LogicalSort(sort0=[$0], dir0=[DESC], fetch=[2:BIGINT])
  LogicalAggregate(group=[{0}], EXPR$1=[COUNT()])
    LogicalProject(dim1=[$2])
      LogicalTableScan(table=[[druid, foo]])
```

The captured plan is staight from Calcite except for `INSERT` queries where, due
to some awkward bits within Druid itself, the `LogicalInsert` operation is
synthetic.

### `execPlan` Section

Represents the "physical plan" for the query or insert. Druid has no real
concept of a physical plan: this is a bit of an artifical concept. For a
Druid query, the `execPlan` is the native query plus a bit more contextual
information which the Broker uses. For an `INSERT` the physical plan is
defined by an extension and includes information about how to run th
insert.

Note that, in Druid, a "union" query is defines as a list of native queries
which the Broker runs on after another. The `execPlan` invents an
"artifical" "union" query to show the list of native queries.

### `resource` Section

Lists the resource actions which the planner is expected to report. A resource
action is a triple of resource type, resource name, and action:

```
=== resources
DATASOURCE/dst/WRITE
EXTERNAL/EXTERNAL/READ
```

Resources must be in the above form with the items separated by slashes.

### `explain` Section

Provides the expected `EXPLAIN SELECT ...` output for a query. Works only if the
query *does not* contain the `EXPLAIN` keyword itself.

```text
=== explain
DruidQueryRel(query=[(
{
  ...
```

Note that, if the query *does* contain the `EXPLAIN` keyword, then you should use
the `results` section to indicate the expecte output of running the `EXPLAIN`
query. (Warning: the output is all on one line and is quite hard to read!)

Note: this section is a work-in-progress.

### `native` Section

Indicates the Druid native query which the query should produce.

```text
=== native
{
  "queryType" : "topN",
  "dataSource" : {
    "type" : "table",
    "name" : "foo"
  },
  ...
```

The native query is in JSON format, pretty-printed with the default Jackson
JSON pretty-printing formatter.

The native query is generally straight out of the Druid planner, except for
two special cases:

* Union queries apparently have no native form: instead they are given as a list
of native queries. For our purposes, we create an "artificial" native query to
hold the members. This is probably wrong, but works for now:

```text
=== native
{
  "artificialQueryType" : "union",
  "inputs" : [ {
    "dataSource" : {
      "type" : "table",
      "name" : "foo"
    },
    ...
```

* `INSERT` queries have no native form at present.

### `results` Section

Provides the results, in JSON-arrays format that the query is expected to produce when run.

```text
=== results
["","a",2.0,2]
["1","a",8.0,2]
```

The `results` section can appear by itself (if there is only one way to run the test)
or inside a `run` section.

### `run` Section

Defines one of several runs for a test. Each typically has an `options` section to
set up the run, and a `results` section that says what results are expected for that
setup. Example:

```text
=== run
=== options
sqlCompatibleNulls=false
=== results
["",1]
["def",1]
=== run
=== options
sqlCompatibleNulls=true
=== results
["def",1]
["abc",1]
```

### `exception` Section

When a query is expected to fail, you can provide the expected exception,
expected error message, or both. Don't specify the other output sections
for queries that fail a plan time. If a query fails at run time, then
you can include the expected plan values. Be sure to include the following
to tell the framework that the error is a runtime exception:

```text
=== options
failure=run
```

Without that, the framework assumes that any error occurs at plan time.

To specify an exception:

```text
=== exception
ValidationException
```

The test passes if the actual exception is of the given class, or has a cause
of the given class.

Note that for some (as yet) unknown reason, the various `Calcite{foo}QueryTest`
classes appear to produce different exception classes than the test framework.
If test cases are verified against the existing case, you can omit the exception
and reference just the error.

### `error` Section

Provides the expected error text for a failure. Follows the same rules as
the `exception` section. You will often want to describe error messages using
regex patterns (see below).

```text
=== error
!.*Column count mismatch in UNION ALL

```

### Pattern Sections

Most expected output sections support regular expressions:

* `ast`
* `unparsed`
* `plan`
* `execPlan`
* `schema`
* `targetSchema`
* `native`
* `explain`
* `error`

In a pattern section, each line of the body matches one or more
lines of the input. Expected lines can be of one of three forms:

* Literal text
* Regular expression
* Skip lines

Literal text is exactly that: lines match exactly (ignoring leading and trailing
whitespace.) Literal lines can start with a backslash to escape the other special
characters below:

```text
\! line must start with !
\* line must start with *
\\ line must start with \
```

Regular expressions are indicated by an initial `!` character, the rest is a [Java
regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

```text
!.* Error: expected one of .* but found foo\(\)
```

Remember to escape special characters. `\Q...\E` are handy to quote blocks of text.

The skip lines indicator lets you skip any number of lines up to the one that matches
the following line (or to the end of the input if there is no following expected line.)

Putting it together:

```text
=== error
!Cannot build plan for query: SELECT.*
**
!.*Possible error: SQL requires union between inputs that are not simple table scans.*
```

### Copy Tests

Often we want to run mutiple variatons of the same test. The various `Calcite<foo>QueryTest`
cases automatically run a query both vectorized and non-vectorized. The tests also run
the same cases with default and SQL-compatible NULL handling, with different expected
outputs.

(This section is also a work in progress: the original copy idea turned out to be
a bit cumbersome and will probably evolve.)

We could handle these by copy/pasting the entire test case, but that gets old (and is
hard to maintain). Instead, every section (except `case`) allows the `copy` argument
which says "I'm the same as that for the previous test." For example to run a test
with vectorization on and off:

```text
==============================================================
Converted from testGroupByDouble()
=== case
Group by double (1)
=== SQL
SELECT m2, COUNT(*)
FROM druid.foo
GROUP BY m2
=== options
replacewithDefault=true
vectorize=false
...
==============================================================
Converted from testGroupByDouble()
=== case
Group by double (2)
=== SQL copy
=== options
replacewithDefault=true
vectorize=force
=== schema copy
=== plan copy
=== native copy
=== results copy
```

Notice that only the `options` section changes.

## Record Existing Tests

The other way to create a test file is by recording an existing Java-based test.
This is a bit involved, but saves a ton of work.

* Pick a test file.
* Enable recording. In `BaseCalciteTest` reverse comments on the following:

```java
  protected CalciteTestRecorder recorder = null;
  //protected CalciteTestRecorder recorder =
  		CalciteTestRecorder.create(
  			CalciteTestRecorder.Option.PLAN_ONLY);
```

* Run the target test file. This produces a file called `target/actual/recorded.case`.
* Modify `TestCaseBuilder` to point to the Java class you want to convert. (See the
  many examples at the top of the file.)
* Run `TestCaseBuilder` to produce a file `target/actual/rewritten.case`.
  This file reorders the tests to match the source code order. If you have an existing
  file, it copies over your SQL and other sections.
* Create a test case by starting with `rewritten.case`. If you make another pass,
  use your IDE's diff tools to sync your test case with the rewritten, recorded
  output.
* Run the test case to produce the values that the Java tests don't capture, such
  as the Calcite plan. Use the diff approach above to copy that data into your
  test case file.
* Run the test case again, and it should pass.

Once your test case file is ready, do a manual pass through the original Java
test file. Copy any comments from the Java to the test case header to preserve
that information.

