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

# Next-Generation Query Engine

This package, and its children, implements a "next generation" version of the
Druid Broker/Historical query engine designed around the classic "operator
DAG" structure. The implementation is part of the "multi-stage query engine"
(MSQE) project proposed for Druid, and focuses on the low-latency query path.

Background information can be found in [Issue #11933](https://github.com/apache/druid/issues/11933).

This version is experimental and is a "shim" layer that works with much of
the existing structure of the scatter/gather engine. It establishes a
beach-head from which functionality will grow to allow multiple stages
beyond just scatter and gather.

## Operator Overview

Druid uses a unique query engine that evolved to serve a specific use case:
native queries with a scatter/gather distribution model. The structure is optimized
for efficiency, but is heavily tied to the scatter-gather model. As we move toward
and MSQE design, we want to revisit the engine architecture.

Most modern database implementations involve a set of *operators*. The planner works
with a set of *logical operators*, such as the `RelNode` objects in Druid's Calcite
planner. The planner then converts these to descriptions of *physical operators* which
comprise the *physical plan*. Distributed engines divide the physical plan into
*fragments* (also called *slices*) which are farmed out to the various execution
nodes. Each fragment consists of a set of one or more operators that execute on the
given node. The execution node converts the fragment descriptions into a set of
concrete operators (called by many names) which then run to perform the desired
operations.

The resulting structure forms a *DAG* (directed acyclic graph), typically in the form
of a tree. A root fragment drives the query. The root fragment lives in the Druid
Broker. Other fragments live in execution nodes (Historicals for Druid.) The set
of fragments forms a tree. In classic Druid, it is a two-level tree: the root fragment
represent the "gather" step, a single set of distributed leaf nodes forms the
"scatter" step. In MSQE there can be additional, intermediate fragments.

Each fragment is itself composed of a DAG of operators, where an operator is a
single relational operation: filter, merge, limit, etc. Each operator reads from
one or more "upstream" operators, does one task, and sends results to the "downstream"
operators. Query engines typically use the "Volcano" iterator-based structure:
each operator has a `next()` method which returns the next results. For performance,
`next()` typically returns a *batch* of rows rather than a single row. In an RDBMS,
the batch might correspond to a single buffer. In Druid, it corresponds to an array
of rows.

The result is that each operator is an interator that returns batches of rows until
EOF. The `Operator` interface has a very simple protocol:

```java
public interface Operator
{
  Iterator<Object> open(FragmentContext context);
  void close(boolean cascade);
}
```

Opening an operator provides an iterator to read results. Closing an operator releases
resources. Notice that the operator returns an iterator, but isn't itself an interator.
This structure allows an operator to be a "pass-through": it does nothing per-row but
only does things at open and close time.

See `Operator`, `NullOperator` and `MockOperator` for very simple examples of the
idea. See `MockOperatorTest` to see how operators work in practice.

## Constast with Sequences and Query Runners

Druid has historically used an engine architecture based on the `QueryRunner` and
`Sequence` classes. A `QueryRunner` takes a native query, does some portion of the
work, and delegates the rest to a "base" or "delegate" query runner. The result is a
structure that roughly mimics that of operator. However, query runners are more
complex than an operator: query runners combine planning and execution, and are
often tightly coupled to their implementation and to their upstream (delegate) query
runners. Operators, by contrast, handle only execution: planning is done by a separate
abstraction. Operators are agnostic about their inputs and outputs: operators can be
composed in any number of ways and can be easily tested in isolation.

Query runners return a `Sequence` which returns results on demand. The query runner is
really a mechanism for building the required `Sequence`, often via a set of lambdas
and anonyous inner classes. The result is that the `Sequence` is tightly coupled with
its query runner. The `Sequence` is also similar to an operator in that it returns
results. An operator, however, is simpler. The `Sequence` class is designed to ensure
that the sequence is always closed and uses a somewhat complex protocol to ensure that.
Operators accomplish the same result by employing a "fragment runner" to create, manage
and close the operators. The result is that the operator version of a task is generally
far simpler than the `Sequence` version.

The `QueryRunner`/`Sequence` implementation tries to be stateless as much as possible,
which just means that state occurs in closure variables within the query runner
implementation. Such closures are hard to debug and make testing complex. Operators, by
contrast, are shamelessly stateful: they manage, via member variables, whatever state
is required to perform their job. Since operators run within a single thread at any
given time, such state is safe and well-defined. (Generally, in a data pipeline, it
doesn't even make sense for two threads to call the same operator: what does it mean
to do two sorts of the same data in parallel, for example?)

The general mapping, in this version of the new engine is:

*  `QueryPlanner` corresponds to `QueryRunner`: it figures out which operators are
needed.
* `Operator` corresponds to `Sequence`: it is the physical implementation of a data
transformation task.

## Support Structure

The above description touched on several of the support classes that support
operators:

* `QueryPlanner`: figures out which operators to create given a native query.
* `FragmentRunner`: runs a set of operators, handles failures, and ensures clean-up.
* `FragmentContext`: fragment-wide information shared by all operators.

## Temporary Shim Structure

Druid is a mature, complex project &mdash; it would be unwise to attempt to upgrade
the entire query stack in one go. Instead, we evolve the code: keep what works well
and replace the bits we need to improve incrementally.

In this step, we replace query runners and their sequences one-by-one. The result is a
bit tricky:

* Druid's query stack still produces the set of query runners, and we still invoke `run()`.
on each.
* When the "next gen" engine is enaled, each query runner delegates to the
`QueryPlanner` to work out an operator equivalent for what the query runner previously
did. When the engine is disabled, the query runner does its normal thing.
* The `QueryPlanner` has a modified copy of the query runner code, but in a form that
works out an operator to use. Sometimes the planner determines that no operator is
needed for a particular query.
* The `QueryPlanner` then creates an operator. Since the outer query runner is tasked
with returning a `Sequence`, we then wrap the operator in a `Sequence` so it fits into
the existing protocol.
* Most query runners have an upstream (delegate, base) runner. In that case, the
`QueryPlanner` calls `run()` on the base, converts its `Sequence` to an `Operator` and
hands that to the newly created operator as its input.

The result is that we've got two parallel structures going: query runners speak `Sequence`
while the `QueryPlanner` speaks `Operator`. To make this work, we use "adapter" to
convert an operator to a sequence and a sequence to an operator. If we literally did
this, every query runner would produce a sequence/operator pair and a chain of two
query runners would look like this:

```text
|--- query runner 1 ----|----- query runner 2 ----|
Sequence <-- Operator <-- Sequence <-- Operator <-- ...
```

Of course, the above would be silly. So, we emply another trick: when we wrap a sequence
in an operator, we check if the sequence in question is one which wraps an operator. If
so, we just discard the intermediate sequence so we get:

```text
Sequence <-- Operator <-- Operator
```

Only the final stage is presented as a sequence: all the internal operations are
structured as operators.

The result is a structure that plays well with both query runners and operators. When
we have a string of operators: we have an operator stack. When we interface to parts of
Druid that want a sequence, we have a sequence.

The operator-wrapping sequence ensures that the operator is closed when the sequence
is closed. The operator cascades the close call to its children. This ensures that the
operator is closed as early as possible.

A fragment context also holds all operators and ensures that they are all closed at
query completion, if not closed previously. In the eventual sructure, this mechanism
will ensure operators close even if an error occurs.

### Next Steps

The purpose of the current, interim structure is simply to introduce operators in a
safe, seamless way, and to enable testing. Later, we want to expand the scope. One way
is to expand the operator structure up through the early native query planning states so
that we produce an operator-based structure that cuts out the (now superflorous) query
runnners.

Later, we can introduce an revised network exchange layer so that we can send a fragment
description to the Historical rather than sending a native query.

During all of this, we will keep the "classic" engine in place, adding the "next gen"
engine alongside.

## Configuration

The "next gen" engine is off by default. Two steps are necessary to enable it:

1. Set the `druid.queryng.enable` property in a config file or the command line.
2. Set the `queryng` context variable to `true` in each query.

These steps are temporary since, at this time, the "next gen" query engine is
experimental.

## Usage

Once enabled, the next gen engine should "just work." There is no user-visible indication
of the new path. Developers can see the difference in stack traces: if an exception is thrown
in the query engine, or you set a breakpoint, you will see "Operator" methods on the stack
rather than "Sequence" or "Yielder" methods.

Other than that, results should be identical. At scale (millions of rows), the operator
structure is more efficent. But, in most queries, the bulk of the time is spent in low-level
segment tasks outside the set of operations which have thus far been converted to operators.

## Tests

One handy feature of operators is that they can be tested independently. A set of tests
exist for each of the currently-supported operators.
