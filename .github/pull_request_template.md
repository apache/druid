Fixes #XXXX.

(Replace XXXX with the id of the issue fixed in this PR. Remove this line if there is no corresponding
issue. Don't reference the issue in the title of this pull-request.)

Add tags to your PR if you are a committer (only committers have the right to add tags). Add [Design Review] tag
if this PR should better be reviewed by at least two people.
Don't forget to add the following tags (if applicable): [Incompatible], [Release Notes], [Compatibility], [Security],
[Development Blocker]. Add at least one [Area - ] tag, consider creating a new one if none of the existing [Area - ]
tags is applicable.

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

If the change involves any design decisions, including:
 - Choosing algorithm of doing something
 - Choosing the Druid behaviour (Should certain configuration values be accepted? What some Druid nodes should do in
   some corner situations, e. g. insufficient resources?)
 - Class organization and design (how the logic is split between classes, inheritance, composition, design patterns)
 - Method organization and design (how the logic is split between methods, parameters and return types)
 - Naming (class, method, API, configuration, HTTP endpoint, names of emitted metrics)

Describe alternative designs (mention alternative names) and compare them with the designs that you've implemented (the
names you've chosen).

If you already did this in the associated issue (e. g. a "Proposal" issue), leave the following sentence:

Design of this change is discussed [here](<link to Github issue or comment where you discuss the design>).

This section may be omitted for really simple and small patches. However, any patch that adds a new production class
almost certainly shouldn't omit this section.

<hr>

I've self-reviewed this PR (including using the [concurrency checklist](
https://medium.com/@leventov/code-review-checklist-java-concurrency-49398c326154)).

Leave the sentence above if you've self-reviewed your PR. Leave the part in parens if your PR has any relation to Java
concurrency (any use of Threads, `synchronized`, `volatile`, `ConcurrentHashMap`, etc.) and you used the referenced
checklist during your self-review.