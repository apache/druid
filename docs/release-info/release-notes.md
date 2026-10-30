---
id: release-notes
title: "Release notes"
---

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

<!--Replace {{DRUIDVERSION}} with the correct Druid version.-->

Apache Druid \{\{DRUIDVERSION}} contains over $NUMBER_FEATURES new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from $NUMBER_OF_CONTRIBUTORS contributors.

<!--
Replace {{MILESTONE}} with the correct milestone number. For example: https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A28.0+sort%3Aupdated-desc+
-->

See the [complete set of changes](https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A{{MILESTONE}}+sort%3Aupdated-desc+) for additional details, including bug fixes.

Review the [upgrade notes](#upgrade-notes) and [incompatible changes](#incompatible-changes) before you upgrade to Druid \{\{DRUIDVERSION}}.
If you are upgrading across multiple versions, see the [Upgrade notes](upgrade-notes.md) page, which lists upgrade notes for the most recent Druid versions.

<!-- 
This file is a collaborative work in process. Adding a release note to this file doesn't guarantee its presence in the next release until the release branch is cut and the release notes are finalized.

This file contains the following sections:
- Important features, changes, and deprecations
- Functional area and related changes
- Upgrade notes and incompatible changes

Please add your release note to the appropriate section and include the following:
- Detailed title
- Summary of the changes (a couple of sentences) aimed at Druid users
- Link to the associated PR

If your release note contains images, put the images in the release-info/assets folder.

For tips about how to write a good release note, see [Release notes](https://github.com/apache/druid/blob/master/CONTRIBUTING.md#release-notes).
-->

## Important features, changes, and deprecations

This section contains important information about new and existing features.

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### Other web console improvements

### Ingestion

#### SQL-based ingestion

##### Other SQL-based ingestion improvements

#### Streaming ingestion

##### Other streaming ingestion improvements

### Querying

#### Stricter `requireTimeCondition` guard

The `druid.sql.planner.requireTimeCondition` guard is now enforced on every physical leaf in the query tree, not just the outermost scan. Previously, a query could pass the guard with only an outer `__time` filter even when an inner subquery or a right-hand join input scanned the entire datasource. The guard now rejects:

- A right-hand join input or semi-join subquery with no `__time` filter, even when the left-hand side is bounded.
- A nested subquery whose innermost query ancestor has no `__time` filter, even when an outer wrapper is bounded.

`GlobalTableDataSource` on the right side of a join is exempt, since it is materialized via `JoinableFactory` as an `IndexedTable` rather than scanned as segments.

[#20441](https://github.com/apache/druid/pull/20441)

#### Other querying improvements

### Cluster management

#### Other cluster management improvements

### Data management

#### Other data management improvements

### Metrics and monitoring

### Extensions

### Documentation improvements

## Upgrade notes and incompatible changes

### Upgrade notes

#### Stricter `requireTimeCondition` enforcement

If `druid.sql.planner.requireTimeCondition=true` is set (or `requireTimeCondition=true` is passed in a query context), the guard now rejects some queries that previously succeeded:

- Queries with a right-hand join input or semi-join subquery that has no `__time` filter, even when the left-hand side is bounded.
- Queries with a nested subquery whose innermost query ancestor has no `__time` filter, even when an outer wrapper is bounded.

Previously, only the outermost scan's segment spec was checked, so these queries would pass validation but still scan the entire datasource at the inner leg. If you rely on `requireTimeCondition` and have queries matching these shapes, add a `__time` filter to the inner subquery or right-hand join input, or wrap it in an inner query whose own interval is bounded.

[#20441](https://github.com/apache/druid/pull/20441)

### Incompatible changes

### Developer notes

#### Dependency updates

The following dependencies have had their versions bumped: