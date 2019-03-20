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

## PR action item checklist for committers

1. Add appropriate tags to the PR, in particular:

     - **`Design Review`** - for changes that will be hard to undo after they appear in some Druid release, and/or
     changes that will have lasting consequences in the codebase. Examples:
        - Major architectural changes or API changes
        - Adding new or changing behaviour of existing query types (e. g. changing an algorithm behind some query type
        or changing from floating point to double precision)
        - Adding new or changing existing HTTP requests and responses (e. g. a new HTTP endpoint)
        - Adding new or changing existing interfaces for extensions (`@PublicApi` and `@ExtensionPoint`)
        - Adding new or changing existing server configuration parameter (e. g. altering the behavior of a config
        parameter)
        - Adding new or changing existing emitted metrics
        - Other major changes

     The PR description should succintly, but completely list all public API elements (@PublicApi or @ExtensionPoint),
     configuration options, emitted metric names, HTTP endpoint paths and parameters that are added or changed in the
     PR. If they are not listed, ask the PR author to update the PR description.

     - **`Incompatible`** - for changes that alter public API elements (@PublicApi or @ExtensionPoint), runtime
     configuration options, emitted metric names, HTTP endpoint behavior, or server behavior in some way that affects
     one of the following:

        - Ability to do a rolling update [as documented](http://druid.io/docs/latest/operations/rolling-updates.html)
        without needing any modifications to server configurations or query workload.
        - Ability to roll back a Druid cluster to a prior version.
        - Ability to continue using old Druid extensions without recompiling them.

     Note that no matter what, we must support the ability to do a rolling update somehow (even if some special care is
     needed), and the ability to roll back to at least the immediate prior Druid version. If a change makes either one
     of these impossible then it should be re-designed.

     All `Incompatible` PRs should be tagged `Design Review` too, but not vice versa: some `Design Review` issues,
     proposals and PRs may not be `Incompatible`.

     - **`Release Notes`** - for important changes that should be reflected in the next Druid’s version release notes.
     Critically, those are changes that require some server or query configuration changes made by Druid cluster
     operators to preserve the former cluster behaviour, i. e. the majority of PRs tagged `Incompatible`. However, some
     `Incompatible` PRs may not need to be tagged `Release Notes`, e. g. PRs that only change some extension APIs,
     because when building extensions with the newer Druid version the incompatibility will be discovered anyway.

     Secondarily, PRs that add new features, improve performance or improve Druid cluster operation experience could
     also be tagged `Release Notes` at your discretion.

     - **`Bug`** / **`Security`** / **`Feature`** / **`Performance`** / **`Refactoring`** / **`Improvement`** - can be
     used to distinguish between types of changes. **`Compatibility`** tag also falls into this category, it’s
     specifically for PRs that restore or improve compatibility with previous Druid versions if it was inadvertently
     broken, or for changes that ensure forward compatibility with future Druid versions, forseening specific changes
     that would otherwise break the compatibility.

     - **`Development Blocker`** - for changes that need to be merged before some other PRs could even be published.
     `Development Blocker` PRs should be prioritized by reviewers, so that they could be merged as soon as possible,
     thus not blocking somebody's work.

     - Consider adding one or several **`Area -`** tags. Consider creating a new `Area -` tag if none of the existing
     `Area` tags is applicable to the PR.

2. Consider adding any `Bug` and `Security` PRs to the next Druid milestone whenever they are important enough to fix
before the next release. This ensures that they will be considered by the next release manager as potential release
blockers. Please don't add PRs that are neither `Bug` nor `Security`-related to milestones until after they are
committed, to avoid cluttering the release manager's workflow.

3. If the PR has obvious problems, such as an empty description or the PR fails the CI, ask the PR author to fix these
problems even if you don't plan to review the PR.

## PR merge action item checklist

1. Add the PR to the next release milestone.

2. If the PR is labeled `Incompatible` and the next release milestone differs from the previous only in the "patch"
number (such as 0.10.1, while the previous release is 0.10 or 0.10.0), rename the next milestone so that it bumps the
"minor" number (e. g. 0.10 -> 0.11).

3. Consider thanking the author for contribution, especially a new contributor.