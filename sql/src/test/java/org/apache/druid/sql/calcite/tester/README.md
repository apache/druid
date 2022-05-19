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

# Druid Planner Tests

The classes in this module provide a test framework for the Druid planner.
The tests are based on "case" files that provide a SQL query and expected
results. The tests themselves are defined as a JUnit test which runs through
all tests in the case file.

Tests primarily check planning artifacts (Calcite plan, native queries, etc.)
but also test semantics via test runs using an abbreviated, in-memory query
engine.

## Running the Tests

Tests run as part of a JUnit test: `DruidPlannerTest`. That file runs each of
the case files, compares actual with expected results, and reports success
or failure. If a case fails, the code writes a new file, with actual results,
in the `target/actual` folder. Use your favorite "diff" tool to compare the
expected and actual files to see what is off. Either fix the issue, or accept
the new behavior, to update the "expected" case file to match the actual
results.

## Writing a Test

Planner tests can be super simple or quite complex. Let's step through the
process. Let's suppose you want to create a new case file. Go ahead and
create one: `src/test/resources/calcite/cases/myCase.case`. Copy the
copyright heading from an existing file.

A case has, at a minimum, a `case` element that names the test, a SQL
statement, and one or more expected results. For example, if we just
want to plan a query and capture the native results:


```text
==============================================================
Example of the world's simplest test case.
=== case
My first test case
=== SQL
SELECT * FROM foo
=== native
...
```

The file has a rather unusual syntax: lines that start with three
equal signs (`===`) indicate sections. Why the odd syntax? Test cases
include JSON, SQL comments and CSV results. We need a syntax that is
very unlikly to collide with these various contents.

In the SQL above `foo` is an in-memory datasource provided by the test
framework. We want to verify the native query. Notice that we've just
left the native query section as "...". That's because we're lazy:
we'll let the test tell us the answer.

Next, we create a driver function. Let's assume we'll add this to the
existing `DruidPlannerTest` file:

```java
  @Test
  public void testMyCase() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/myCase.case")
        .run(standardBuilder()));
  }
```

Run this in your IDE as a JUnit test. It will fail, but that's what we expect.
Find `target/actual/myCase.case`. Open it in an editor and use your "diff"
tool to copy over the actual native query (after eyeballing it to make sure
it is correct.) Run the test again. Now it passes.

Congrats, you've created your first test! Of course, they're not all this
easy. Let's dive into the details.

## Capture More Planning Artifacts

In addition to the native query, we can also capture:

* Parser AST (abstract syntax tree): `ast` section.
* "Unparse" of the SQL, to see what Calcite things we said: `unparse` section.
* The output schema: `schema` section.
* The resource actions (the datasources that the query uses and the kind of
access: READ or WRITE): the `resources` section.
* The Calcite logical plan: the `plan` section.
* The explained plan: the `explain` section. This section is almost the same
as the result of an `EXPLAIN` query, with a few minor differences. The framework
formats the output so it is *far* easier to read than running an actual `EXPLAIN`
query and comparing a huge, long results line.

For example, to capture some of these, add the following to our test
case:

```text
=== SQL
SELECT * FROM foo
=== schema
...
=== plan
...
=== native
```

Again, run the query, compare the actual results, and copy over the actuals
to become the expected values. Here's an example:

```text
=== SQL
SELECT DISTINCT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
=== schema
SCHEMA_NAME VARCHAR
=== plan
BindableAggregate(group=[{1}])
  BindableTableScan(table=[[INFORMATION_SCHEMA, SCHEMATA]])
```

The above works with Druid's "virtual" `INFORMATION_SCHEMA`.
There is no native query for such queries.

## Query Context

Queries are planned with an empty query context. For one thing, this keeps
the captured native queries simple. Your test may want to change a context
value, such as forcing the "current date" to some specific value. You do that
using the `context` section:

```text
=== context
sqlCurrentTimestamp=2000-01-01T00:00:00Z
```

The syntax is like a properties file: `key=value`. Strings need not be
quoted unless they start or end with spaces. The test framework will
convert the value to the right type based on metadata which appears
in `QueryContexts`. If you add a new non-string context variable,
you may need to update the metadata for the value to parse
correctly.

## Run Tests

Thus far we've talked about capturing planning artifacts. The many existing
JUnit tests also capture query results, using an abbreviated test-specific
execution engine. Let's do that:

```
=== SQL
SELECT DISTINCT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
=== results
["lookup"]
["view"]
["druid"]
["sys"]
["INFORMATION_SCHEMA"]
```

The above shows that, if we run the specified query, we expect to get
the results shown. In practice, you can use the same trick as above:
add a `results` section, run the query, and copy results from the
actual output file.

### Vectorization

The test framework always runs queries (at least) two ways: with vectorization
off, and with it on. When we run test, we should specify whether the query
is vectorizable or not. We do that with the `options` section: instructions
to the test framework itself. For example:

```text
=== options
vectorize=true
```

Or

```text
=== options
vectorize=false
```

Most test explain why they can't be vectorized:

```text
==============================================================
Converted from testEarliestAggregatorsNumericNulls()

Cannot vectorize EARLIEST aggregator.
=== case
Earliest aggregators numeric nulls
=== SQL
SELECT EARLIEST(l1), EARLIEST(d1), EARLIEST(f1)
FROM druid.numfoo
=== options
vectorize=false
```

When `vectorize` is `true`, the framework runs the test once with
`vectorize=FALSE`, a second time with `vectorize=FORCE`. If the
`vectorize` option is `false`, then only the first is done:
`vectorize=False`.

### SQL-Compatible Nulls

Druid has an unusual feature: it can use "classic" "null" handling in
which a blank string (or 0 numeric) is considered the same as SQL `NULL`,
or "SQL compatible" mode in which SQL `NULL` is a distinct value. In
the happy path, the query produces the same results either way and
we use an option, `sqlCompatibleNulls=both`, to say so:

```text
=== SQL
SELECT DISTINCT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
=== options
sqlCompatibleNulls=both
vectorize=true
```

### Debug Mode

When the tests are run in Maven on Travis, then Travis will run the entire
set of unit tests with SQL-compatible mode enabled, then another time with
the mode disabled. This is the default behavior of the test framework.

When working in an IDE, it is a pain to have to change the debug setup for
tests to try both modes. To avoid that, the framework recognizes a special
system property:

```text
-Ddruid.debug=true
```

Set that (once) in your IDE for the `DruidPlannerTest` setup. Then, the
tests will, internally, run all tests once with SQL-compabible mode, a second
time with "classic" (replace nulls with defaults) mode.

### Differing Run Results

From here the story gets pretty complex, so get ready. In some cases,
only the query results differ. In this case, we specify multiple query
"runs" per test case using the `run` section. For example:

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

Each `run` section can have a name if we like:

```text
=== run
Results for SQL-compatible mode.
```

However, in the above, the meaning is clear from the options so we skip the
name. Each `run` section can also specify options which are scoped to just
that run. Each run happens with the "main" options overridden with the
per-run options. In the above, we had one run with SQL-compatible nulls,
the other without. There are cases where we want to very other options
as well, but that gets pretty advanced.

Then, we list the results for that specific configuration.

As it turns out, the null mode is baked deeply into Druid: it is the kind of
option you want to select at the first installation, then never change. At
run-time, the null-handling model is a global setting: there is no way to
change it per query (or per datasource). So, how to the test handle this?
Very carefully, and with several hacks, as it turns out. The test framework
runs all tests in a case file with SQL-compatible nulls turned off, then
runs them again with SQL-compatible nulls enabled. (If your counting, we're
up to four runs of each test case for nulls and vectorization.)

The `sqlCompatibleNulls` option acts like a filter: the test framework
skips tests (or runs) that don't match the current null-handling option.
(The `both` value matches both settings.)

### Comparing Floating-point Values

If your test expects floating-point values (`float` or `double`), which
are not nice, even integers (such as `10.0`), then you cannot do the default
text-based comparison of results. You have to tell the framework to do the
slow, complex, typed comparsion:

```text
=== options
typedCompare=true
```

This flag causes the expected values to be parsed as JSON into Java objects,
then compares floating-point values using a delta of 1%. If you find special
cases that also need handling, modify the rather baroque class
`LinesSection.JsonComparsionCriteria` to handle those cases.

### Differing Plan Artifacts

In many cases, the null-handling option affects not just the results, but
also the native plan. There is no "planning" section like there is a `run`
section: there is only one set of planning artifacts per test case. But, all
is not lost: we just create two cases, and tell the framework to copy over
the common parts:

```text
=== case
My case
=== SQL
SELECT ...
=== options
sqlCompatibleNulls=false
=== schema
foo VARCHAR
=== native
<native query for "replaceNullsWithBlanks" mode.>
=== case
My Case
=== SQL copy
=== options
sqlCompatibleNulls=true
=== schema copy
=== native
<native query for "SQL-compatile" mode.>
```

When we pull this trick, we put the results in the respective test case with no
need for a `run` section. (The `run` section is needed only if there are two
or more runs per test case.)

### Join Option Generator

Many join-related tests use a JUnit parameter annotation to repeat tests with
various query contexts:

```java
  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOuterGroupByAndSubqueryNoLimit(
  		Map<String, Object> queryContext) throws Exception
```

The test framework provides the same mechanism via an option:

```text
=== options
provider=QueryContextForJoinProvider
```

The name is the same as the Java class, but this is just a convention: the
actual generator is hard-coded. (If you need a different one, you'll have to
add yours to the `QueryTestCaseRunner` class.) The result is that the test
is run as many times as there are options variations. There are eight variations
for `QueryContextForJoinProvider` (three Boolean context variables.) Since
we said we already did four variations, we now have a total of 32 runs of
queryies that use this option. That should test the heck out of any query!

You'll see some tests that use variations on this theme, but where the plan
is different depending on the options. A special generator generates the
"plan A" option variations, while the opposite generates "plan B." This is
an obscure feature: you can ignore it until you need it.

## Handling Errors

Good test cover not only the "happy path", but also test for errors. Every
test case (or run) either suceeds or fails. If it suceeds, you use the sections
above. If it fails, you use the `exception` and `error` sections:

```text
=== exception
ValidationException
=== error
!.*Column count mismatch in UNION ALL
```

The rule for `exception` is that the actual exception must be, or derive from,
the exception named in the section. The `error` section gives the text of the
error.

Put these two sections in the main body for plan-time errors. Place them in
a `run` section for run-time errors.

### Regular Expressions

This is a good time to introduce a "special feature" of the framework: regular
expressions. Almost every expected values section (that is, those that are
outputs from Druid rather than inputs to Druid) can contain regular expressions.
The one exception is the `results` section.

By default, lines are treated as literals. But, if a line starts with an
exclamation point, the rest of the line is a regular expression. In the example
above: we match away all the detailed cruft in the error message up to the
part that conveys the essential meaning.

Consult the [Java regular expression syntax](
https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)
for details. The most useful patterns are `.*` (match anything) and
`\Q...\E` (match everything between the two markers.) Many messages contain
regular expression characers (such as parens, brackets, etc.) Use a backslash,
or that quote syntax, to escape them.

There is one other handy feature: a line with just `**` matches any number of
actual lines. This is occasionally useful. It would be more useful if we recorded
just the essential bits of a native query and not the entire text. Perhaps we only
care that a particular filter was added. The existing tests are all literal captures
of native queries coded in Java, and so don't use the `**` feature as much as they
probably should.

Finally, any line that starts with a backslash is a literal, starting with the
second character. This is how you match `!foo`, for example. See
[internals.md] for more details.

## Investigate Issues

You've written some tests, added more sections, or added results, and something
breaks. Or, you've tinkered with the planner code, and query results change. How
do you track down problems?

If the test succeeds, you'll get a JUnit pass and you can be confident that
that things work. But, if something changed, you'll get a failure. The JUnit
run itself will just tell you that the test failed. The test framework produces
a file to explain exactly what went wrong. The file is in
`target/actual/<case>.case`: that is, the same name as the case file, but in
the target directory.

To see what changed, use your IDE or favorite diff tool to compare the two
files. The "actual" file has a header per case that identifies failures, and
then lists the actual output. Inspect the differences.

If the difference is expected (that is, you just made a change that
intentionally caused the difference), then copy the actual input over to the
test case as the new "expected" value. On the other hand, if you didn't
expect the change, you've got to track down what went wrong.

Sometimes the failure is due to an unstable JSON serialization. (Java `Set`s,
for example, have a non-deterministic serialization order.) For sanity, we
want to fix serialization so it is deterministic. Other times, the there is
a difference because of something that changes from run to run, such as the
query ID. Find a way to hold the value constant, or use regular expressions
(see below) to pattern-match and work around the values that change.

Most times, however, something broke unintentionally. Debug the problem,
make a fix and try the test again until it is happy.

## Create a New Test - Advanced

You've seen the test creation process step-by-step. Once you've done one, you
will want to skip the steps and just cut to the chase.

To create a new test, you can do it the hard way or the easy way. The hard way is
to work out the expected values for each section and spell that out in the test.
The easy way is to let the computer do the work for you. Specify the inputs, but
provide bogus values for the outputs. For a query that should succeed:

```text
==============================================================
<comments here>
=== case
<test case name here>
=== SQL
<SQL here>
=== options
sqlCompatibleNulls=both
vectorize=true
=== schema
foo
=== resources
foo/foo/READ
=== plan
foo
=== native
foo
=== results
foo
```

Fill in the three `<...>` sections above. (The comments are optional, but
helpful.) Alter the options as needed. Add any context settings. The general
convention is to use the separator line between cases, add a comment, list
the SQL, then context, then options. Expected result sections follow, in
roughly the order that Druid produces them: the ast, unparsed SQL, the
schema, resources, plan and native query. Results (and runs, if needed)
appear list. Sort options in alphabetical order.

The above conventions are not required, but they will make it easier to
view the actual results file. (It is also the order that cases are generated if
you convert a JUnit test. See the [internals.md](internals.md) file for the
details of JUnit conversion.)

Then, run the test. It will, of course, fail. It will produce an
actual output file (as described above). Open that in your IDE then compare
your test case (expected) file with the actual file. Use your IDE to copy
across the actual values, making those the expected values. Run the test
again. It should now pass. Note: don't copy blindly: inspect to ensure that
the actual values are, in fact, what you expect.

If you expect an error, use this template instead:

```
==============================================================
<comments here>
=== case
<test case name here>
=== SQL
<SQL here>
=== exception
foo
=== error
foo
```

As it turns out, you will get different exceptions if you run the test in
the test framework than if you run it in the `BaseCalciteQueryTest` framework.
An easy workaround is to omit the exception and check only the error text.

## JUnit Test Case

The Planner tests use a number of internal classes. The `PlannerFixture` is the
core: it allows your code to configure the planner however you need it for your
tests. A `Builder` lets you choose options, otherwise the fixture uses the same
defaults and mock elements used by `BaseCalciteTest`.

```java
public class DruidPlannerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // Converted from CalciteInsertDmlTest
  @Test
  public void testInsertDml() throws IOException
  {
    PlannerFixture.Builder builder = new PlannerFixture
        .Builder(temporaryFolder.newFolder())
        .withView(
            "aview",
            "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'");
    QueryTestSet testSet = QueryTestSet.fromResource("/calcite/cases/insertDml.case");
    assertTrue(testSet.run(builder));
  }
```

The easiest approach is to add your test case as another method within
`DruidPlannerTest`: there is no advantage to creating a separate JUnit test
class. Your method will suceed if all tests pass, fail if any test fails.
Use the `target/actual/<case>.case` file to locate actual failures.

The intro section showed an abbreviated way to write the test if you use the
"standard" builder. The one here shows how to customize the builder. Note that
you pass the *builder*, not the *built* object into the test runner. The test
runner will use the builder multiple times to build the world first without
SQL-compatible nulls, then again with them.

You may find you have to extend the JUnit test case if the Java tests are doing
something special. For example, you can use `PlannerFixture` to create custom
schemas, load any needed views, set default query context options, and so on.

You may also have to extend the test framework itself if you need new options
for special cases not yet covered, or other kinds of unusual cases.

## Status

This framework is new. At present, the framework duplicates the tests from the various
JUnit `CalciteXQueryTest` cases. Once the framwork is solid, we'll make a final
conversion pass of any newly added or change tests the, if the team agrees, we'll
deprecate the existing tests so we don't have to keep the two sets in sync.

The framework as support for the new `INSERT` syntax via the `targetSchema`
section. However, the required support is not quite ready in Druid so this part
is a work-in-progress.
