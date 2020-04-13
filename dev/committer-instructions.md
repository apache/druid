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

This document contains various instructions relevant for Druid committers.

## PR and issue action item checklist for committers

This checklist describes steps that every committer should take for their own issues and PRs and when they are the first
committer who visits an issue or a PR authored by a non-committer.

1. Add appropriate labels to the PR, in particular:

   - [**`Design Review`**](https://github.com/apache/druid/labels/Design%20Review) - for changes that will be
   hard to undo after they appear in some Druid release, and/or changes that will have lasting consequences in the
   codebase. Examples:
     - Major architectural changes or API changes
     - Adding new or changing behaviour of existing query types (e. g. changing an algorithm behind some query type or
     changing from floating point to double precision)
     - Adding new or changing existing HTTP requests and responses (e. g. a new HTTP endpoint)
     - Adding new or changing existing interfaces for extensions (`@PublicApi` and `@ExtensionPoint`)
     - Adding new or changing existing server configuration parameter (e. g. altering the behavior of a config
     parameter)
     - Adding new or changing existing emitted metrics
     - Other major changes
  
     PRs that flesh out community standards, checklists, policies, and PRs that change issue and PR templates, in other
     words, many of the PRs labelled `Area - Dev` should often be labelled `Design Review` as well.

     The PR description should succinctly, but completely list all public API elements (`@PublicApi` or
     `@ExtensionPoint`), configuration options, emitted metric names, HTTP endpoint paths and parameters that are added
     or changed in the PR. If they are not listed, ask the PR author to update the PR description.

   - [**`Incompatible`**](https://github.com/apache/druid/labels/Incompatible) - for changes that alter public
   API elements (`@PublicApi` or `@ExtensionPoint`), runtime configuration options, emitted metric names, HTTP endpoint
   behavior, or server behavior in some way that affects one of the following:

     - Ability to do a rolling update [as documented](https://druid.apache.org/docs/latest/operations/rolling-updates.html)
     without needing any modifications to server configurations or query workload.
     - Ability to roll back a Druid cluster to a prior version.
     - Ability to continue using old Druid extensions without recompiling them.

     Note that no matter what, we must support the ability to do a rolling update somehow (even if some special care is
     needed), and the ability to roll back to at least the immediate prior Druid version. If a change makes either one
     of these impossible then it should be re-designed.

     All `Incompatible` PRs should be labelled `Design Review` too, but not vice versa: some `Design Review` issues,
     proposals and PRs may not be `Incompatible`.

   - [**`Release Notes`**](https://github.com/apache/druid/labels/Release%20Notes) - for important changes
   that should be reflected in the next Druid’s version release notes. Critically, those are changes that require some
   server or query configuration changes made by Druid cluster operators to preserve the former cluster behaviour, i. e.
   the majority of PRs labelled `Incompatible`. However, some `Incompatible` PRs may not need to be labelled
   `Release Notes`, e. g. PRs that only change some extension APIs, because when building extensions with the newer
   Druid version the incompatibility will be discovered anyway.

     Secondarily, PRs that add new features, improve performance or improve Druid cluster operation experience could
     also be labelled `Release Notes` at your discretion.

   - [**`Bug`**](https://github.com/apache/druid/labels/Bug) / [**`Security`**](
   https://github.com/apache/druid/labels/Security) / [**`Feature`**](
   https://github.com/apache/druid/labels/Feature) / [**`Performance`**](
   https://github.com/apache/druid/labels/Performance) / [**`Refactoring`**](
   https://github.com/apache/druid/labels/Refactoring) / [**`Improvement`**](
   https://github.com/apache/druid/labels/Improvement) - can be used to distinguish between types of changes.
   [**`Compatibility`**](https://github.com/apache/druid/labels/Compatibility) label also falls into this
   category, it's specifically for PRs that restore or improve compatibility with previous Druid versions if it was
   inadvertently broken, or for changes that ensure forward compatibility with future Druid versions, forseening specific
   changes that would otherwise break the compatibility.

   - [**`Development Blocker`**](https://github.com/apache/druid/labels/Development%20Blocker) - for changes
   that need to be merged before some other PRs could even be published. `Development Blocker` PRs should be prioritized
   by reviewers, so that they could be merged as soon as possible, thus not blocking somebody's work.

2. If you added some labels on the previous step, describe why did you do that, either in the PR description (if you are
the author of the PR) or in a comment (if you have added labels to a PR submitted by someone else).

3. Consider adding one or several **`Area -`** labels to the PR or issue. Consider [creating a new `Area -` label](
#creating-a-new-label-on-github) if none of the existing `Area` labels is applicable to the PR or issue.

   - [`Area - Automation/Static Analysis`](
   https://github.com/apache/druid/labels/Area%20-%20Automation%2FStatic%20Analysis) - for any PRs and issues
   about Checkstyle, forbidden-apis, IntelliJ inspections, code style, etc. Should also be used for PRs and issue
   related to TeamCity CI problems.
   - [`Area - Cache`](https://github.com/apache/druid/labels/Area%20-%20Cache) - for PRs and issues related to
   Druid's query results cache (local or remote). Don't use for PRs that anyhow relate to caching in different contexts.
   - [`Area - Dev`](https://github.com/apache/druid/labels/Area%20-%20Dev) - for PRs and issues related to the
   project itself, such as adding developer's docs and checklists, Github issue and PR templates, Github-related issues.
   Don't use for PRs and issues related to CI problems: use either `Area - Testing` for problems with Travis or
   `Area - Automation/Static Analysis` for problems with TeamCity. PRs with `Area - Dev` label should usually change
   files in `dev/` or `.github/` directories.
   - [`Area - Documentation`](https://github.com/apache/druid/labels/Area%20-%20Documentation) - for PRs and
   issues about Druid's documentation for users and cluster operators. Don't use for PRs and issues about the
   documentation of the Druid's development process itself: use `Area - Dev` for that purpose. Don't use for issues and
   PR regarding adding internal design documentation and specification to code, usually, in the form of Javadocs or
   comments (there is no specialized label for this).  
   - [`Area - Lookups`](https://github.com/apache/druid/labels/Area%20-%20Lookups) - for PRs and issues
   related to Druid's Query Time Lookups (QTL) feature.
   - [`Area - Metadata`](https://github.com/apache/druid/labels/Area%20-%20Metadata) - for PRs and issues
   related to the organization and contents of the metadata store, the metadata store itself, and managing the metadata
   in the memory of various Druid nodes.
   - [`Area - Null Handling`](https://github.com/apache/druid/labels/Area%20-%20Null%20Handling) - for PRs and
   issues related to the [Null Handling project](https://github.com/apache/druid/issues/4349).
   - [`Area - Operations`](https://github.com/apache/druid/labels/Area%20-%20Operations) - for PRs and issues
   related to Druid cluster operation process, for example, PRs adding more alerting, logging, changing configuration
   options.
   - [`Area - Query UI`](https://github.com/apache/druid/labels/Area%20-%20Query%20UI) - for issues that
   mention or discuss the questions related to presenting Druid query results for human perception.
   - [`Area - Querying`](https://github.com/apache/druid/labels/Area%20-%20Querying) - for any PRs and issues
   related to the process of making data queries against Druid clusters, including the PRs and issues about query
   processing and aggregators.
   - [`Area - Segment Balancing/Coordination`](
   https://github.com/apache/druid/labels/Area%20-%20Segment%20Balancing%2FCoordination) - for PRs and issue
   related to the process of loading and dropping segments in Druid clusters according to specified *rules*, and
   balancing segments between Historical nodes in clusters. Coordinator node is responsible for both processes. This
   label is not called "Area - Coordinator" because Coordinator has some other duties that are not covered by this
   label, for example, compacting segments.
   - [`Area - Testing`](https://github.com/apache/druid/labels/Area%20-%20Testing) - use for any PRs and
   issues related to testing (including integration testing), Travis CI issues, and flaky tests. For flaky tests, also
   add [`Flaky test`](https://github.com/apache/druid/labels/Flaky%20test) label.
   - [`Area - Zookeeper/Curator`](https://github.com/apache/druid/labels/Area%20-%20Zookeeper%2FCurator) - for
   any PRs and issues related to ZooKeeper, Curator, and node discovery in Druid.


4. **Consider adding any `Bug` and `Security` PRs to the next Druid milestone** whenever they are important enough to
fix before the next release. This ensures that they will be considered by the next release manager as potential release
blockers. Please don't add PRs that are neither `Bug` nor `Security`-related to milestones until after they are
committed, to avoid cluttering the release manager's workflow.

5. If the PR has obvious problems, such as an empty description or the PR fails the CI, ask the PR author to fix these
problems even if you don't plan to review the PR.

6. If you create an issue that is relatively small and self-contained and you don't plan to work on it in the near
future, consider labelling it [**`Contributions Welcome`**](
https://github.com/apache/druid/labels/Contributions%20Welcome) so that other people know that the issue is
free to pick up and is relatively easily doable even for those who are not very familiar with the codebase.

## PR merge action item checklist

1. Add the PR to the next release milestone.

2. If the PR is labeled `Incompatible` and the next release milestone differs from the previous only in the "patch"
number (such as 0.10.1, while the previous release is 0.10 or 0.10.0), rename the next milestone so that it bumps the
"minor" number (e. g. 0.10 -> 0.11).

3. Check that the issue addessed in the PR is closed automatically by Github. If it's not, close the issue manually.

4. Consider thanking the author for contribution, especially a new contributor.

## Creating a new label on Github

After creating a new label for your PR or issue, don't forget to take the following steps:

1. Search existing PRs and issues and try to add the new label to at least three of them.

2. Describe the new label in the [PR and issue action item checklist for committers](
#pr-and-issue-action-item-checklist-for-committers) above.

3. Add the label to stalebot's excemptions if needed: see [`stale.yml`](../.github/stale.yml).

4. Although it is *not* necessary to preliminarily discuss creation of a new label in the developers' mailing list,
please announce the new label after you've created it at `dev@druid.apache.org`.

## Become an Administrator of the Druid project in TeamCity

Druid committers shall obtain a status of a [Druid project](
https://teamcity.jetbrains.com/project.html?projectId=OpenSourceProjects_Druid)
administrator. See the [corresponding section in the document about TeamCity](
teamcity.md#becoming-a-project-administrator) for details.