<!-- Thanks for trying to help us make Apache Druid be the best it can be! Please fill out as much of the following information as is possible (where relevant, and remove it when irrelevant) to help make the intention and scope of this PR clear in order to ease review. -->

<!-- Please read the doc for contribution (https://github.com/apache/druid/blob/master/CONTRIBUTING.md) before making this PR. Also, once you open a PR, please _avoid using force pushes and rebasing_ since these make it difficult for reviewers to see what you've changed in response to their reviews. See [the 'If your pull request shows conflicts with master' section](https://github.com/apache/druid/blob/master/CONTRIBUTING.md#if-your-pull-request-shows-conflicts-with-master) for more details. -->

Fixes #XXXX.

<!-- Replace XXXX with the id of the issue fixed in this PR. Remove this section if there is no corresponding issue. Don't reference the issue in the title of this pull-request. -->

<!-- If you are a committer, follow the PR action item checklist for committers:
https://github.com/apache/druid/blob/master/dev/committer-instructions.md#pr-and-issue-action-item-checklist-for-committers. -->

### Description

<!-- Describe the goal of this PR, what problem are you fixing. If there is a corresponding issue (referenced above), it's not necessary to repeat the description here, however, you may choose to keep one summary sentence. -->

<!-- Describe your patch: what did you change in code? How did you fix the problem? -->

<!-- If there are several relatively logically separate changes in this PR, create a mini-section for each of them. For example: -->

#### Fixed the bug ...
#### Renamed the class ...
#### Added a forbidden-apis entry ...

<!--
In each section, please describe design decisions made, including:
 - Choice of algorithms
 - Behavioral aspects. What configuration values are acceptable? How are corner cases and error conditions handled, such as when there are insufficient resources?
 - Class organization and design (how the logic is split between classes, inheritance, composition, design patterns)
 - Method organization and design (how the logic is split between methods, parameters and return types)
 - Naming (class, method, API, configuration, HTTP endpoint, names of emitted metrics)
-->


<!-- It's good to describe an alternative design (or mention an alternative name) for every design (or naming) decision point and compare the alternatives with the designs that you've implemented (or the names you've chosen) to highlight the advantages of the chosen designs and names. -->

<!-- If there was a discussion of the design of the feature implemented in this PR elsewhere (e. g. a "Proposal" issue, any other issue, or a thread in the development mailing list), link to that discussion from this PR description and explain what have changed in your final design compared to your original proposal or the consensus version in the end of the discussion. If something hasn't changed since the original discussion, you can omit a detailed discussion of those aspects of the design here, perhaps apart from brief mentioning for the sake of readability of this PR description. -->

<!-- Some of the aspects mentioned above may be omitted for simple and small changes. -->

#### Release note
<!-- Give your best effort to summarize your changes in a couple of sentences aimed toward Druid users. 

If your change doesn't have end user impact, you can skip this section.

-->
For tips about how to write a good release note, see [Release notes](https://github.com/apache/druid/blob/master/CONTRIBUTING.md#release-notes).


<hr>

##### Key changed/added classes in this PR
 * `MyFoo`
 * `OurBar`
 * `TheirBaz`

<hr>

<!-- Check the items by putting "x" in the brackets for the done things. Not all of these items apply to every PR. Remove the items which are not done or not relevant to the PR. None of the items from the checklist below are strictly necessary, but it would be very helpful if you at least self-review the PR. -->

This PR has:

- [ ] been self-reviewed.
   - [ ] using the [concurrency checklist](https://github.com/apache/druid/blob/master/dev/code-review/concurrency.md) (Remove this item if the PR doesn't have any relation to concurrency.)
- [ ] added documentation for new or modified features or behaviors.
- [ ] a release note entry in the PR description.
- [ ] added Javadocs for most classes and all non-trivial methods. Linked related entities via Javadoc links.
- [ ] added or updated version, license, or notice information in [licenses.yaml](https://github.com/apache/druid/blob/master/dev/license.md)
- [ ] added comments explaining the "why" and the intent of the code wherever would not be obvious for an unfamiliar reader.
- [ ] added unit tests or modified existing tests to cover new code paths, ensuring the threshold for [code coverage](https://github.com/apache/druid/blob/master/dev/code-review/code-coverage.md) is met.
- [ ] added integration tests.
- [ ] been tested in a test Druid cluster.
