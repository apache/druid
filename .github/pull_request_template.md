Fixes #XXXX.

(Replace XXXX with the id of the issue fixed in this PR. Remove this line if there is no corresponding
issue. Don't reference the issue in the title of this pull-request.)

(If you are a committer, follow the PR action item checklist for committers:
https://github.com/apache/incubator-druid/blob/master/dev/committer-instructions.md#pr-and-issue-action-item-checklist-for-committers.)

### Description

Describe the goal of this PR, what problem are you fixing. If there is a corresponding issue (referenced above), it's
not necessary to repeat the description here, however, you may choose to keep one summary sentence.

Describe your patch: what did you change in code? How did you fix the problem?

If there are several relatively logically separate changes in this PR, list them. For example:
 - Fixed the bug ...
 - Renamed the class ...
 - Added a forbidden-apis entry ...

Some of the aspects mentioned above may be omitted for simple and small PRs.

### Design

Please describe any design decisions made, including:
 - Choice of algorithms
 - Behavioral aspects. What configuration values are acceptable? How are corner cases and error conditions handled, such
   as when insufficient resources are available?
 - Class organization and design (how the logic is split between classes, inheritance, composition, design patterns)
 - Method organization and design (how the logic is split between methods, parameters and return types)
 - Naming (class, method, API, configuration, HTTP endpoint, names of emitted metrics)

It's good to describe an alternative design (or mention an alternative name) for every design (or naming) decision point
and compare the alternatives with the designs that you've implemented (or the names you've chosen) to highlight the
advantages of the chosen designs and names.

If there was a discussion of the design of the feature implemented in this PR elsewhere (e. g. a "Proposal" issue, any
other issue, or a thread in the development mailing list), link to that discussion from this PR description and explain
what have changed in your final design compared to your original proposal or the consensus version in the end of the
discussion. If something hasn't changed since the original discussion, you can omit a detailed discussion of those
aspects of the design here, perhaps apart from brief mentioning for the sake of readability of this PR description.

This section may be omitted for really simple and small patches. However, any patch that adds a new production class
almost certainly shouldn't omit this section.

<hr>

This PR has:
- [ ] been self-reviewed.
   - [ ] using the [concurrency checklist](
   https://github.com/apache/incubator-druid/blob/master/dev/code-review/concurrency.md) (Remove this item if the PR
   doesn't have any relation to concurrency.)
- [ ] added documentation for new or modified features or behaviors.
- [ ] added Javadocs to non-trivial members.
- [ ] added code comments for hard to understand areas.
- [ ] added unit tests or modified existing tests to cover new code paths.
- [ ] added integration tests.
- [ ] been tested in a test environment.
- [ ] been tested in a production environment.

(Check the items by putting "x" in the brackets for the done things. Not all of these items apply to every PR.)