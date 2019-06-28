# Druid development principles

This document enumerates the principles that Druid developers should follow if they want to
 - Sustain long-term maintainability of the project.
 - Improve the project effectively.
 - Improve their own and fellow developers' skill. This will have positive second-order effects in the perspective of
 the previous two goals.
 - Minimize overhead associated with distributed, cross-organizational development.
 - Foster the atmosphere of trust and respect in the project development. This will have positive second-order effects
 in the perspective of all previous goals.

## 1. Diligence

<a name="research"/>
<h3><a href="#research">#</a> 1.1. Research the question</h3>

Before creating an issue, or a discussion thread, or coding something, research the question:

 - Search for prior discussions or mentions of the problem on Github, [the development forum](
 https://lists.apache.org/list.html?dev@druid.apache.org), and [the old development forum](
 https://groups.google.com/forum/#!forum/druid-development).
 - For relatively high-level and important problems, search for how similar problems are solved in other projects,
 especially those very similar to Druid: [Pinot](https://github.com/apache/incubator-pinot), [Kylin](
 https://github.com/apache/kylin), and [ClickHouse](https://github.com/yandex/ClickHouse), or any other projects where
 you can find relevant information.
 - If applicable, search for the latest scientific results on the topic.

<h4>1.1.a. Link to all prior resources and discussions</h4>

Add references to all prior resources and discussions that you've found to the issue, or the post in the discussion
thread, or the PR that you are creating.

<h4>1.1.b. Link to related issues, PRs, and discussions</h4>

If during your research you have found some prior issues, PRs, or discussions which are not exactly about the problem
you are looking at but related, link to them from your posting as well.

<a name="review-problem-and-solution"/>
<h3><a href="#review-problem-and-solution">#</a> 1.2. Review the problem and the solution</h3>

When reviewing other people's work and reviewing your own work on some problem, ask the following questions:

 - Does the problem really need to be solved?
 - Doesn't the solution to the problem have a negative effect (usually on the complexity of the system or the cumulative
 future support costs of the new feature) that outweighs the benefit from solving the problem?
 - Does the proposed solution actually solve the problem?
 - *Are there radically different (perhaps non-code) approaches to solving the problem?* For example, problems can be
 eliminated by removing the problematic functionality, changing guarantees, requirements, assumptions, protocols, or
 configurations. How do those approaches compare to the proposed solution?

<a name="design"/>
<h3> <a href="#design">#</a> 1.3. Design deliberately</h3>

Deliberately think about the design of your solution. Consider alternative designs and discuss the pros and the cons of
the alternative designs compared to your final design in the PR description.

<a name="focus-areas"/>
<h4><a href="#focus-areas">#</a> 1.3.a. Choose focus areas</h4>

One approach to deliberate design thinking is choosing a few focus areas where you want to the project to improve and
check in the perspective of those areas *every* solution that you create or review.

Let's assume that the focus areas are *fault-tolerance* and *ease of cluster operation* (note: this is just an example;
every developer and organization may have different focus areas). Then, someone who has chosen these focus areas could
ask themselves the following questions regarding every solution that they create or review:

 - How fault-tolerant is this solution? How does it contribute to the reliability of the whole system? Will the system
 continue to work (serve queries, ingest data, etc.) if this feature stops working?
 - Does this solution (feature) make operating Druid clusters easier or harder?

<h4>1.3.c. Pay close attention to hard-to-undo things</h4>

Be especially meticulous when reviewing things that once committed into the codebase (or appear in a Druid's release)
will be hard or impossible to undo or rework, in other words, things with long-term compatibility impact. Those include

 - New API endpoints (including the format of the input, the format of the output, and the return codes), especially
 client-facing endpoints.
 - New column or aggregation type.
 - New Druid query.
 - On-disk formats, including [segment format](https://github.com/apache/incubator-druid/issues/5347).
 - Etc.

Consider the implications and the limitations (if any) of the proposed designs of such things in the perspective of your
personal [focus areas](#focus-areas) as well as generally important areas, such as *security*, *fault-tolerance*,
*performance*, *ease of cluster operation*, and *usability for clients*.

<a name="review-api">
<h4><a href="#review-api">#</a> 1.3.c. Review the class and method organization, interfaces, and naming</h4>

Give some thought to the structure and boundaries of interfaces and classes in the code. Ask yourself:

 - If this breakdown of the logic between classes and methods optimal?
 - Don't some classes or methods outgrown a reasonable size and should be split up between smaller entities?
 - Is this interface design optimal?
 - Does the code using these interfaces appear OK or there are immediate code smells in it, such as repetition, or
 user-side code not understandable due to poor method naming?

As a PR author, you can do this naturally when you [make summaries of newly added interfaces, classes, methods, and
renamed entities](#api-summary) in the description of your PR.

<a name="comment-document"/>
<h3><a href="#comment-document">#</a> 1.4. Comment and document abundantly</h3>

<a name="explaining-comments"/>
<h4><a href="#explaining-comments">#</a> 1.4.a. Make the code obvious by adding a lot of explaining comments</h4>

When writing new code or modifying existing Druid's code, imagine some other developer who is unfamiliar with this code
and this particular part of the system (you can only assume their general familiarity with Druid project structure and
basic concepts) reading it. Will there be places where the developer would need to perform a mini-investigation in order
to be able to understand the intent of the code and why it looks the way it does? Add explaining comments to all such
places so that an unfamiliar developer wouldn't have to "figure things out" as they read the code.

As a PR reviewer, also [suggest explaining comments](#suggest-explaining-comments) to be added to the code.

<a name="add-javadocs"/>
<h4><a href="#add-javadocs">#</a> 1.4.b. Add Javadoc comments to all classes and non-trivial methods</h4>

Add Javadoc comments to all classes and all non-trivial methods.

 - Explain the purpose of the class and link to other classes or places in the code where it is used.
 - A good comment for a "dull" configuration parameter holder class may describe what is being configured and how these
 can be configured for a Druid node. For example, a comment for `AWSClientConfig` could have been
 `General configuration of the client connection to AWS S3 when using as deep storage. Can be configured for Druid nodes
 via druid.s3.*`
 - A comment to a class as trivial as a no-op implementation of some strategy can at least link to the more interesting
 class where it is applied: `/** Used in {@link MoreInterestingClass}. */`.

<a name="javadoc-links"/>
<h4><a href="#javadoc-links">#</a> 1.4.c. Interconnect the codebase using Javadoc links</h4>

 - Always make comments to classes *and fields* Javadoc comments: `/** ... */` rather than plain comments: `// ...`.
 - Whatever members (classes, methods, and fields) you mention in your comments, always make them Javadoc links:
 `{@link ...}`.
 - Shorten method links by removing the parameters: `{@link Set#add}` (instead of `{@link Set#add(Object)}`).
 - Even when commenting on fields which are adjacent in the class declaration, their descriptions should refer to each
 other using Javadoc links. This will prevent "documentation rot" when one of the fields is renamed or removed.
 - Non-trivial methods are those for which it's not obvious from their name and the signature what do they do and how
 they should be implemented. Getters and setters are typically trivial.

<a name="document-comprehensively"/>
<h4><a href="#document-comprehensively">#</a> 1.4.d. Document features, configuration parameters, and API endpoints
comprehensively</h4>

For every feature, document (apart from the functional description of the feature):
 - The tradeoffs of using the feature or turning the feature on/off. For example,
 `turning feature X on YYY Druid nodes increases residual memory usage by O(N), where N is the number of segments in the
 cluster`. Or,
 `turning feature X on YYY Druid nodes makes impossible to upgrade Druid cluster from version A to version B without
 downtime (when nodes ZZZ are upgraded, YYY nodes with this feature on will not work well)`.
 - The behavior in all possible edge cases.

For every configuration parameter, document:

 - The motivation for adding this parameter. Under what conditions a cluster operator would want or need to change the
 value of this parameter.
 - If known, the tradeoffs in play when the parameter's value is changed. For example,
 `Increasing this parameter's value reduces the tail latency of XXX Druid nodes' responses to requests YYY but the nodes
 consume more CPU while idle`. Quantify the effects if possible:
 `the latency will be less than X milliseconds, but idle CPU usage increases from Y to Z%`.
 - If known, the maximum/minimum acceptable and reasonable values, for example:
 `It's unlikely that anyone would ever need to set this value higher than X`.
 - If the configuration parameter is added just in case of some unexpected production failure scenario, *document this
 fact explicitly*:
 `This parameter's value shouldn't normally be changed. The parameter is added in case of unexpected production failure
 scenarios. If you had to change this parameter's value to normalize operation of a Druid cluster, please let know Druid
 developers.`

For every internal or external API endpoint, document:

 - The semantics, the scope and the purpose of the API. Who (or what node) is going to use it.
 - Request payload and response formats.
 - All possible return error codes and the behavior of the endpoint in edge cases.

Documentation for an internal API endpoint could be put into a Javadoc comment for the corresponding implementation
method in a `*Resource` class.

<a name="comment-todo"/>
<h4><a href="#comment-todo">#</a> 1.4.e. Comment at to-be-improved places</h4>

When you know that the code can be improved in a certain way, but don't do it because of time limitations, or because
you want to keep your PR shorter, [create an issue](https://github.com/apache/incubator-druid/issues/new) describing the
improvement and *link to the issue from a comment in the code*. This applies even when you are planning to work on this
improvement yourself just after finishing the current PR. Sometimes PRs take much longer to merge than expected, and
priorities change quickly.

Use this strategy instead of adding `TODO` comments.

<a name="fix-all-occurrences"/>
<h3><a href="#fix-all-occurrences">#</a> 1.5. Find and fix all occurrences of a mistake</h3>

While working on your PR, whenever you notice a typo, an API misuse, a defect, or any other problem which doesn't appear
very unique, search the codebase for other occurrences and fix them immediately [if the fixes wouldn't massively
outweigh the essential changes in your PR](#cleanup-as-you-go).

If you think such fixes would make your PR [too large](#small-pr) or too distracted from the PR's main target open an
issue for doing such fixes instead as per principle [Open issues regarding fixing similar defects, root causes, and
possible static analysis checks](#open-issues-defects-static-analysis).

Apart from fixing all existing occurrences of a bug in the codebase (or opening an issue for doing that), also [think
about the root cause of the bug](#analyze-bug) and remedy the root cause.

<a name="search-rename"/>
<h3><a href="#search-rename">#</a> 1.6. Search the codebase manually on symbol and concept renames and member
deletions</h3>

Whenever you rename a member (field, method, class) don't rely just on the rename refactoring in your IDE. Search the
codebase manually for the member's name. There are often references which are not formatted as Javadoc links
(`{@link }`) which the refactorings in IDEs may skip.

When renaming a field whose name is common in the codebase (such as `segments`), invoke a textual search within the
class.

As you update the textual references, [turn them into Javadoc links](#javadoc-links) as per principle [Fix small things
as your work with code](#small-fixes).

<h4>1.6.a. Search the codebase on member deletions</h4>

Whenever you delete a field (method, class, configuration parameter, API endpoint, etc.), manually search the codebase
for the residual references to the removed member in comments.

<h4>1.6.b. Use full-text search on Github when renaming concepts</h4>

When renaming a concept in the database, such as node's name (*Compute* -> *Historical* is an example of such rename in
the past), a task name, an operation name (usually a verb phrase, for example, ["delete segments" -> "mark segments as
unused"](https://github.com/apache/incubator-druid/pull/7306)), or a subconcept which is identified by a noun phrase
(such as "deleted segment" -> "unused segment"), use [full-text search on Github](
https://github.com/apache/incubator-druid/search) which may help to find partial occurrences, occurrences with unusual
word order and variation phrase occurrences. Try plural forms and past verb forms yourself because Github's search
doesn't recognize those.

<a name="tests"/>
<h3>1.7. Invest in tests</h3>

<h4>1.7.a. Create unit test-friendly classes and methods</h4>

<ul>
<li>
Extract the logic that solves a more abstract problem as a class or a method which is easily testable from the glue code
responsible for integrating that logic into the system:
<ul>
<li>Dependency injection</li>
<li>Reading and supplying configuration parameters</li>
<li>Coupling with other "big" classes</li>
</ul>

The rest of the system would still talk to the "glue" entity as before. The "glue" entity would delegate the
implementation to the "general" entity. The general class or method would be used only from the "glue" class or method
and from unit tests.

This pattern is also called [Humble Object](http://xunitpatterns.com/Humble%20Object.html).
</li>

<li>
Extract parts of logic from larger methods into separate methods if they are self-contained, even if this logic is not
used in any other place. These extracted methods can then be unit-tested independently from the larger methods.
</li>
</ul>

<h4>1.7.b. Add integration tests for all new logic involving remote communication in some way</h4>

This includes:

 - New API endpoints
 - New types of tasks or ingestion features
 - New logic related to service discovery or ZooKeeper
 - Etc.

<a name="test-duplication"/>
<h4><a href="#test-duplication">#</a> 1.7.c. Eliminate duplication code in tests early</h4>

Don't copy-paste the code in tests (or type the same code by hand). Eagerly extract helpers to remove duplication of
scaffolding and boilerplate code in tests.

<h4>1.7.d. Improve the quality of the testing infrastructure</h4>

See principles [Increase automation of the project regularly](#automate) and
[Open issues about flaky tests](#flaky-test).

<a name="review-levels"/>
<h3><a href="#review-levels">#</a> 1.8. Review on different levels</h3>

The point of this principle is that PRs should be reviewed on *both* high and low levels, not just the high level or
just the low level.

<a name="high-level-review"/>
<h4><a href="#high-level-review">#</a> 1.8.a. Understand the problem and review the solution</h4>

Immerse yourself into the problem that the author of the PR is trying to solve. Ask yourself (or the author of the
solution) the questions from principles [Review the problem and the solution](#review-problem-and-solution) and [Design
deliberately](#design).

<a name="line-by-line-review"/>
<h4><a href="#line-by-line-review">#</a> 1.8.b. Read the code line by line</h4>

If you encounter anything that you don't understand while reading the code, leave a comment like
`I don't understand this code. Could you make the code more obvious or add a code comment explaining it?` (as per
principle [Make the code obvious by adding a lot of explaining comments](#explaining-comments)).

<h3>1.9. Check that the work follows these principles and use checklists</h3>

Check that the PR follows the principles outlined above in this section:

 - Does the issue or the PR description include some [background research](#research)?
 - Is [some attention to design](#design) paid in the descriptions (or comments inside the code itself), for example, in
 the form of [discussing design decisions](#explain-design) or [presenting alternatives](#alternatives)?
 - Are there enough [explaining comments](#explaining-comments), [javadocs](#add-javadocs), and
 [documentation](#document-comprehensively)?
 - Are there enough [tests](#tests)?

Apart from this short list of questions and the lists from principles
[Review the problem and the solution](#review-problem-and-solution) and [Review the class and method organization,
interfaces, and naming](#review-api), [use the proper project's checklists](#use-checklists).

Note that this principle only concerns how to ensure that PRs are of a specific level of quality consistently, not
whether all (or any) PRs *should* have that level of quality. For example, you may consciously tolerate some PRs not
having a lot of tests (or any tests), or very thorough design work, or very comprehensive documentation. You may choose
the level of quality that a PR should have to depend on its area (in particular, whether the PR changes something in the
core Druid, or core extensions, or contrib extensions), whether the PR changes something in the scope of essential
Druid's operations (ingestion and querying) or some non-essential operations (such as metrics and logging), the author
(you may hold a higher standard for PRs from people who are regular Druid contributors than for PRs from first-time
contributors), and some other factors.

The reason why this principle exists is that after you have decided a level of quality for a PR, it's still
embarrassingly easy to overlook the things mentioned above during a review despite there is just a handful of them and
you may have done them yourself many times.

<h2>2. Proactivity</h2>

<a name="self-review"/>
<h3><a href="#self-review">#</a> 2.1. Review your PRs yourself</h3>

Don't expect that anybody will find bugs in your code for you. If you want to commit code with fewer bugs go over your
PRs yourself line-by-line.

It may help to do this a little while after publishing and in a different interface (e. g. IDE vs. Github) to see the
code with fresher eyes.

Note: even if you are a committer, self-reviews don't count towards the minimum levels required for PR merging: all code
should still be reviewed line-by-line by at least one other committer (only there may be different committers reviewing
different parts of the PR), and the PR should still have one high-level design approval from another committer, or two
high-level design approvals if the PR (or the corresponding issue) has a `Design Review` or a `Proposal` tag.

<a name="open-more-issues"/>
<h3><a href="#open-more-issues">#</a> 2.2. Open more issues yourself</h3>

The idea of this principle is that whenever you notice something that worth being tracked in a separate issue which
doesn't yet exist, *open one yourself not waiting or expecting anybody else to do that.* Even if the necessity for an
issue becomes apparent to you while reading a conversation between other people in some issue or PR which you don't
participate, create an issue and post a link to it in the respective thread if nobody else has done that yet. Don't be
afraid to "sneak the idea" by opening an issue which somebody else discovered.

Also, don't hesitate to open an issue if you are unsure if something is indeed an issue or a thing that works as
expected. It's fine to open question issues or issues that will be closed as "won't fix". Don't "pre-approve" issues,
open them right away. See also principle [Describe ideas in issues even if you are not planning to work on
them](#describe-ideas).

<h4>2.2.a. Open issues regarding to-be-improved things</h4>

Whenever you notice something that can be improved in the codebase, either while reviewing someone else's PR or while
working on your code or just while browsing the codebase, search for existing issues regarding this (as per principle
[Research the question](#research)) and if there is nothing, create a new issue. Do it yourself even when the issue
regards the code newly added in the PR and "it was the author's job" to open such an issue according to principle
[Comment at to-be-improved places](#comment-todo).

<a name="open-issues-defects-static-analysis"/>
<h4><a href="#open-issues-defects-static-analysis">#</a> 2.2.b. Open issues regarding fixing similar defects, root
causes, and possible static analysis checks</h4>

Whenever you spot a style violation, an API misuse, or some other defect in code which looks like a pattern, either
while reviewing someone else's PR or while working on your code or just while browsing the codebase, open an issue
regarding

 1. Finding and fixing other places in the codebase where this problem pops up. (You could also find and fix those
 places yourself immediately if you are working on your PR as per principle [Find and fix all occurrences of a mistake
 ](#fix-all-occurrences).)
 2. If the defect is a bug, fixing the [root cause of the bug](#analyze-bug). Typically, a separate issue is warranted
 when fixing the bug (and other instances of it if appear in the codebase) is relatively easy while fixing the root
 cause is hard: for example, it requires a big refactoring.
 3. Adding a static analysis rule (either [Checkstyle](../codestyle/checkstyle.xml), [forbidden-apis](
 ../codestyle/druid-forbidden-apis.txt), [error-prone](https://errorprone.info/bugpatterns), or [IntelliJ's Structural
 Search inspection](teamcity.md#creating-a-custom-inspection-from-a-structural-search-pattern)) that would prevent such
 defects in the future.

If you don't see how preventing such problems in the future can be automated via static analysis tools (or it would take
too much effort, such as creating a new Checkstyle check from the ground up) [describe the pattern informally in a new
checklist item](#add-checklist-item).

<h4>2.2.c. Open follow-up issues for PRs</h4>

If during a review of a PR it is realized that there are some problems with it which would be too difficult to resolve
within the PR (see principle [Limit the amount of code added to a PR after publishing](#limit-pr-drift)), create an
issue about resolving these problems and assign the PR author to it.

<a name="flaky-test"/>
<h4><a href="#flaky-test">#</a> 2.2.d. Open issues about flaky tests</h4>

When your PR fails CI in some unrelated test search if there is already an issue about it, and if there is no issue,
create one with a `Flaky test` tag.

<h3>2.3. React to review comments proactively</h3>

While working through the comments left to your PR by reviewers, try to stop and *respond to every comment proactively
rather than reactively*.

<h4>2.3.a. When a review comment points to a defect, search for other similar defects in your PR</h4>

When a reviewer points to just one place in code with a defect or a code style violation, or a typo, proactively search
for other similar defects (code style violations, typos) in your PR and in the codebase. Fix all occurrences in the code
changed or added in your PR. Consider also fixing all occurrences in the codebase right in your PR; or open an issue
regarding fixing them (and statically enforcing that similar defects don't creep into the code in the future, if
possible) as per principle [Open issues regarding fixing similar defects, root causes, and possible static analysis
checks](#open-issues-defects-static-analysis).

<h4>2.4.b. When a review comment points to a bug, analyze and fix the underlying cause of it</h4>

See principle [Analyze the root causes of every bug](#analyze-bug).

<a name="update-code-on-questions"/>
<h4><a href="#update-code-on-questions">#</a> 2.3.c. Change the code (or add code comments) when a reviewer asks
anything about it</h4>

Whenever a reviewer asks something about the PR expressing that they don't understand something about the code or the
design, change the code (or add more comments, or links between elements, as per principle [Comment and document
abundantly](#comment-document)) so that the reviewer wouldn't have that question if they reviewed the new version of the
code because it would be  more obvious.

Don't wait for the reviewer to ask you to add a code comment, do it yourself anyway. When a reviewer asks a question,
*your response should be in code* by default rather in the comment thread in the Github's PR web interface.

<a name="internalize-review-comments"/>
<h4><a href="#internalize-review-comments">#</a> 2.3.d. Internalize knowledge expressed via review comments
deliberately</h4>

After your PR is approved, go over all review comments once again and try to *consciously internalize the knowledge
contained in the comments* regarding high-level design problems, insufficient documentation, coding antipatterns, 
efects, API misuse, suboptimal code, style violations, and any other types of problems, so that you would spot such
problems yourself next time you will see code with them while reviewing someone else's PR, or reviewing your own code
yourself (or would avoid such problems while writing your code in the first place).

When a PR is reviewed by several people, reviewers should also go over comments by each other which they wouldn't leave
because they didn't notice the problems.

<a name="analyze-bug"/>
<h3><a href="#analyze-bug">#</a> 2.4. Analyze the root causes of every bug</h3>

Whenever you find a bug in your own code during a self-review or in someone else's code, apart from [fixing all
instances of the bug in the codebase](#fix-all-instances) or [raising issues about doing that and about creating a
static analysis rule](#open-issues-defects-static-analysis), look if you can identify the root cause (or an important
contribution factor) of the bug.

It can be said about most bugs that the reason was a lack of attention from a developer or conflicting changes, and,
perhaps, nothing more can be said about some of the bugs, check if there is something that made the bug easier to let
into the code, for example:

 - Error-prone interfaces: method overloading, parameters of the same type to a method whose order may be confused, etc.
 - Unclear names of local variables, fields, or methods.
 - A too complex (too big) method, or a class, or too complex state and state transitions in a class.
 - Duplication of logic in multiple places, or tangled/inverse logic in two places without comments like
 `Logic in method X/Y must be updated alongside this method` in both places. (For bugs of the kind "code in not all
 necessary places was updated".)

If you find one (or several) of such problems to be present around or related to the code with the bug, fix (if
possible) the root cause in your own code, or ask the author of the PR to fix it, or create a corresponding issue:

 - Make interfaces more robust: avoid overloading, use objects instead of primitives, use builders instead of
 constructors with a lot of parameters, etc.
 - Make the names of variables more precise and clearer.
 - Break up a too complex method or a class into smaller entities.
 - Eliminate logic duplication, or at least add a comment like
 `This code must be changed in parallel with places A, B, and C` alongside *all* places where the repetitive, tangled,
 or inverse logic appears.

<a name="give-answers"/>
<h3><a href="#give-answers">#</a> 2.5. Don't ask questions. Give answers</h3>

<h4>2.5.a. Answer yourself</h4>

Whenever reasonable, research things for yourself, form your own opinion, come up with a solution (which seems best to
your understanding) and post it in a PR or an issue or a discussion thread. Avoid asking other people questions,
especially open-ended ones (those that cannot be answered with a single word, like "yes" or "no"). For example, instead
of posting

 - `@somebody what do you think?`

Write, for example,

 - `I've researched this question and it seems to me there are two viable approaches to solve this problem: X and Y. X
 looks better to me in our case because of Z.`

Or, if you don't have resources to research the question yourself, or you are genuinely interested in the opinion of
someone about the subject, prefer posting non-question statements like the following:

 - `@someone might have a better understanding of this subject.`
 - `I think @someone's perspective on this question would be important because they use this feature a lot.`

There are two exceptions to this principle:

 - [Probe other people's reasoning](#probe-reasoning).
 - [Ask people to do something in their PRs in a polite question form](#be-polite).

<a name="constructive-fud"/>
<h4><a href="#constructive-fud">#</a> 2.5.b. Express fear or doubt in a constructive way</h4>

For example, instead of posting

 - `I'm not sure if that's a good way to proceed.`

Write something like

 - `Somebody should research this question. I don't plan to do it myself now. If nobody does this I would vote for not
 merging this PR because proceeding without knowing the answer to the problem X presents a high long-term
 maintainability risk if it turns out that the answer is Y.`

See also principle [Explore and expose the uncertainty actively](#uncertainty).

<h4>2.5.c. Don't ask questions out of curiosity</h4>

Avoid asking questions the answers to which are not required for proceeding with decisions, solutions, designs, or code.
More generally, avoid posting anything rhetoric, not targeted towards completion of the issue, the PR, or the discussion
thread.

If some answer to your question will mean that there is some problem in the codebase, or that some improvement to the
codebase is possible, post the question as a separate issue rather than a comment in some other issue and describe the
consequences of certain answers to your question.

<a name="#cleanup-as-you-go"/>
<h3><a href="#cleanup-as-you-go">#</a> 2.6. Leave the code cleaner than you found it</h3>

While working on a PR, strive to make the code (method, class, module) that you are changing cleaner than you found it,
even if it means that your PR will have more changed lines than it would otherwise.

Many people think that it's not good when a PR has only a little "essential" changes and the majority of changed lines
because of "auxiliary" changes. Yet, 100% "essential" changes and none "auxiliary" changes is not a healthy proportion
either. Overhead of a PR in Druid is too high for creating a lot of very small PRs with incremental minor fixes, so
those minor fixes should be integrated into larger PRs with more tangible purposes.

<a name="#small-fixes"/> 
<h4><a href="#small-fixes">#</a> 2.6.a. Fix small things as your work with code</h4>

Including:

 - Typos
 - Code style violations
 - Badly formatted code
 - Small inefficiencies
 - Antipattern code
 - Minor defects
 - Inspection warnings (strive for "no yellow highlighting" in your IDE)
 - Comments to fields, methods, and classes which are ordinary Java comments rather than Javadoc comments (see principle
 [Interconnect the codebase using Javadoc links](#javadoc-links))
 - References to methods or classes in Javadoc comments which are not formatted using `{@link }` but appear in plaintext
 (see principle [Interconnect the codebase using Javadoc links](#javadoc-links))

<h4>2.6.b. Rename methods and classes with unfortunate names</h4>

If you encounter a method or a class with a misleading name which doesn't make sense (at least in the current state of
the codebase) or misleading, give them more meaningful and/or accurate names and add Javadoc comments.

<h4>2.6.c. Simplify tricky code and add explaining comments</h4>

If while working on a PR you encounter some expression, part of code, or class without comments that you couldn't easily
understand and has spent some time figuring things out, take time to simplify the logic and/or add [explaining comments
](#explaining-comments) so that future readers (including likely yourself) don't have to spend that time again and again
when working with the code.

<a name="do-optional"/>
<h3><a href="#do-optional">#</a> 2.7. Do everything "optional" that reviewers ask you to do</h3>

When reviewers of your PR leave comments where they ask you to do something, always do that independently of whether the
reviewer marked his comment as "optional" or not.

See also principles [Don't argue unless you disagree on the point](#argue-on-disagreeement) and [Respect reviewer's
requests](#respect-review).

<a name="responsibility-and-leadership"/>
<h2><a href="#responsibility-and-leadership">#</a> 3. Responsibility and leadership</h2>

The following three principles suggest to do something regularly:

 - [Review other people's PRs regularly](#review-regularly)
 - [Increase automation of the project regularly](#automate)
 - [Repay technical debt regularly](#repay-tech-dept)

It may be difficult to put these principles into practice because there are always more important things to do. Yet,
these activities must be done regularly to make your and other people's work with the project efficient and to save time
in the long run. The principles don't specify how much time one should spend on them: it may be as little as 1 hour in
two weeks. But not doing any one of them at all is not sustainable.

One thing that may help to remember doing regular activities is putting recurring time blocks in your calendar.

<a name="review-regularly"/>
<h3><a href="#review-regularly">#</a> 3.1. Review other people's PRs regularly</h3>

Don't just work and publish your stuff and never review other people's work. Review at least as much work (code and
designs) as you do yourself. Ideally, review twice as much work as you do yourself, and prioritize reviewing other
people's work high.

One way to put this principle into practice is choosing a PR (or two PRs) and establishing your intent to review them
(for example, by leaving a comment `I'm going to review this PR`) just before publishing your own PR. Then follow those
PRs according to principle [Once started reviewing a PR, complete your review, or withdraw
explicitly](#review-withdraw).

<h4>3.1.a. Review PRs by people from different organizations</h4>

Don't review just the work published by people from your company. Regularly review PRs and proposals by people working
for other companies.

<h4>3.1.b. Review even if you are not a committer</h4>

You can and should review other people's work regularly even if you are not (yet) a committer and thus your reviews
don't have a formal voting power towards merging PRs.

<a name="#automate"/>
<h3><a href="#automate">#</a> 3.2. Increase automation of the project regularly</h3>

Regularly take time to work on PRs which increase automation of the project, for example, in one of the following ways:

 - Automate something about the project's continuous integration or testing process, or make them run faster.
 - Fix one of the [flaky tests](
 https://github.com/apache/incubator-druid/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22Flaky+test%22).
 - Enable a static analysis check (either via [Checkstyle](../codestyle/checkstyle.xml), [forbidden-apis](
 ../codestyle/druid-forbidden-apis.txt), [error-prone](https://errorprone.info/bugpatterns), or [IntelliJ's Structural
 Search inspection](teamcity.md#creating-a-custom-inspection-from-a-structural-search-pattern)) to prevent some kind of
 defects or API misuse to be committed into the repository in the future.
 - Enable some style check via [Checkstyle](../codestyle/checkstyle.xml) or update Druid's code style settings for
 [IntelliJ](../dev/druid_intellij_formatting.xml) or [Eclipse](../dev/eclipse_formatting.xml) to make it easier for
 developers to follow the Druid's code style.

<a name="repay-tech-dept"/>
<h3><a href="#repay-tech-dept">#</a> 3.3. Repay technical debt regularly</h3>

Regularly dedicate time to do a PR that is devoted specifically to repaying technical dept, for example:

 - [Add Javadocs](#add-javadocs) and [explaining comments](explaining-comments) to a class or a method that you recently
 drained some time understanding.
 - Improve documentation of some feature, configuration parameter, or API endpoint according to principle [Document
 features, configuration parameters, and API endpoints comprehensively](#document-comprehensively).
 - Add unit tests to an area of code with poor test coverage.
 - Add more integration tests.
 - Refactor existing unit tests and integration tests to [remove duplication](#test-duplication), and, more generally,
 to "build up the testing framework" to make adding more tests easier in the future.
 - [Fix concurrency in one of old Druid classes](https://github.com/apache/incubator-druid/issues/7061).

Note: this principle doesn't tell that you shouldn't do *all* of the activities listed above regularly, or ever; but you
should do *some of them* or something similar regularly.

<h3>3.4. Use committer's rights and perform committer's duties responsibly</h3>

If you are a committer, follow the [committer's instructions](committer-instructions.md) regarding issues and PRs
responsibly:

<a name="ensure-review"/>
<h4><a href="#ensure-review">#</a> 3.4.a. Ensure that PRs are reviewed appropriately</h4>

 - Assign appropriate tags, most importantly, `Design Review` and `Incompatible`.
 - Before merging a PR, *make sure that at least one person (apart from the author) [went through it
 line-by-line](#line-by-line-review)*. Don't merge PRs with only high-level or superficial approvals.
 - Before merging a PR with a `Design Review` tag (or when the corresponding issue has a `Design Review` or a `Proposal`
 tag, although in this case the PR must have a `Design Review` tag as well: add it) make sure that it has at least two
 high-level design approvals.

<h4>3.4.b. Don't assign PRs to the next release milestone unless they meet the criteria</h4>

Don't add PRs that are neither `Bug` nor `Security`-related to milestones until after they are committed, to avoid
cluttering the release manager's workflow. See [committer's instructions](committer-instructions.md#milestone).

<h3>3.5. Verify that PR authors have done what they claim</h3>

When a PR author responses "Done" or something similar to your review comment, verify that the latest version of the
patch actually includes the changes.

This is not because of suspecting developers to be deceptive, but because it does happen sometimes that PR authors
forget to commit some changes or to push the branch.

<h3>3.6. Double-check that all instances of a mistake are fixed and all occurrences of a symbol are renamed</h3>

When reviewing a PR in which some mistake (defect, API misuse, antipattern, typo, etc.) is fixed, *verify yourself* that
there are no more instances of this mistake in the codebase in the PR branch. Similarly, when reviewing a PR in which
some members or concepts are renamed or removed, manually search the codebase for residual occurrences yourself.

In other words, follow the principles [Find and fix all occurrences of a mistake](#fix-all-occurrences) and [Search the
codebase manually on symbol and concept renames and member deletions](#search-rename) not only as a PR author but as a
reviewer as well.

If you have found more instances of a mistake in the codebase or residual references to a renamed or removed entity in
comments, along with letting know the PR author about them, leave a comment like this:

 - `When you fix a mistake, please search the codebase for other instances of it. See
 https://github.com/apache/incubator-druid/tree/master/dev/principles.md#fix-all-occurrences`

Or,

 - `When renaming (removing) an entity, please search the codebase for textual references manually. See
 https://github.com/apache/incubator-druid/tree/master/dev/principles.md#search-rename`

<a href="lead-yourself"/>
<h3><a href="#lead-yourself">#</a> 3.7. Always follow these principles no matter if other people do that</h3>

Use these principles in all Druid development and communication regardless of whether the other parties (reviewers,
authors, discussion participants) also follow the principles or not. Set an example and lead yourself. [Respect people's
right not to follow these principles](#respect-principles-ignoring).

<h4>3.7.a. Don't rely on other people to put the principles into practice</h4>

The principles are redundant: even if only one participant in an issue or PR (either author or reviewer) the outcome
should be the same as if all participants followed the principles. Don't count on this: always be that participant who
asks the [conceptual](#review-problem-and-solution) and [design](#design) questions, [checks that the PR is reviewed
](#ensure-review) on [different levels](#review-levels), [features are documented comprehensively
](#document-comprehensively), etc.

<h2>4. Operational principles</h2>

Operational principles aim at minimizing the overhead of highly asynchronous collaboration in issues and PRs.

<h3>4.1. Open issues before doing a lot of work</h3>

Create an issue before starting work on something (code or design) that you think will take you substantial time to
complete. Briefly describe the problem and the solution that you are planning to work out (or, at least, a direction in
which you are looking for a solution). There are two reasons for doing this:

 - Signal the fact that you are working on a problem and avoid a potential clash with someone else.
 - Other developers may chime in with ideas and questions early on which may affect the direction of your work and save
 you time.

If you are working on a design rather than code, you can later post your results in the same issue that you opened
before instead of creating another separate "proposal" issue.

<a name="describe-ideas"/>
<h4><a href="#describe-ideas">#</a> 4.1.a. Describe ideas in issues even if you are not planning to work on them</h4>

Whenever you have an idea (even a vague one) about any new feature in Druid or how the project or a particular place in
the codebase can be improved, don't hesitate to open an issue (or add a comment to an existing issue if there is one
relevant). Ideas *shouldn't* be "thought out well" to be worth sharing. See also principle [Open more issues yourself
](#open-more-issues).

<a name="small-pr">
<h3><a href="#small-pr">#</a> 4.2. Split your work into reasonably-sized PRs</h3>

<h4>4.2.a. While working on a large project, think early about how you are going to spit you work in reasonably-sized
chunks</h4>

Before starting to work on a large project, plan how to divide the work into more or less logically distinct parts. Then
implement the parts and publish them as PRs one by one.

If you won't do that, you may find yourself in a situation when your changeset is already 5000+ lines and growing, but
splitting it into two or more separate changeset is almost as hard as rewriting everything from scratch.

<a name="limit-pr-drift"/>
<h4><a href="#limit-pr-drift">#</a> 4.2.b. Limit the amount of code added to a PR after publishing</h4>

It's not always possible to avoid this completely because some important realizations may come after the PR is published
(without which the original PR doesn't make sense, or posting the follow-up changes in a separate PR would incur too
much overhead), but try to keep these changes to a minimum and don't extend or drift the scope of the PR as it was
originally published.

This is especially important if someone has already started reviewing your PR.

<h3>4.3. Don't split your work into <i>too</i> small PRs</h3>

Because of huge operational overhead of every PR, splitting changes into too many parts may have a negative impact on
the overall overhead of the process. Also, it may be more difficult for other people to see the whole picture in a
string of too finely cut PRs and therefore review your work effectively.

Depending on the specific change (how self-contained and conceptually separated the partial changesets can be), changes
of less than 1000 changed LOC might be best submitted in a single PR. This probability rises for smaller sizes, and it's
likely almost effective to split changes of less than 100 changed LOC into separate PRs.

<h4>4.3.a. Don't split the code so that the initial part(s) are harmful without the final part, or have unwanted
compatibility implications if they appear in a Druid release without the final part</h4>

Don't divide the code into PRs so that they don't make sense in separation. It's more important to keep PRs (at least)
non-harmful than to keep them small: if you don't see a reasonable way to split your big patch into smaller PRs proceed
to publish it as a single PR.

<h3>4.4. Make the descriptions of your PRs helpful for reviewers</h3>

<h4>4.4.a. Describe the conceptual results and connect them with specific places in specific files</h4>

Describe the problems solved by the PR and locate the classes or methods changes in which lead to that. Examples:

 - "Fixed Coordinator disconnection problem by adding retries in method `Foo.bar()`."
 - "Fix a bug in `XxxQuery`"
 - "Added metric `/new/query/metric` (see changes in `XxxQueryMetrics`)"

<h4>4.4.b. Put everything that warrants design review into the PR description</h4>

If the PR should be tagged `Design Review` copy the descriptions of new API endpoints, on-disk and wire communication
formats, changes in `@PublicApi` and `@ExtensionPoint` classes, or anything else that warrants design review of the PR
into the description.

<a name="api-summary"/>
<h4><a href="#api-summary">#</a> 4.4.c. Make summaries of newly added interfaces, classes, methods, and renamed
entities</h4>

<ul>
<li>

When adding new code, help the reviewers (and yourself) to get a bird's eye view on the structure of the code (as per
principle [Review the class and method organization, interfaces, and naming](#review-api)) by adding a condensed
description of newly added interfaces and classes, for example:

<pre><code class="language-java">interface MyNewInterface {
  void remove(List<MyEntry> entries);
  void add(MyEntry newEntry);
}

class MyEntry {
  int foo;
  String bar;
}
</code></pre>

</li>

<li>
If some public classes or methods are renamed in the PR, list the renames in the description, for example:

 - `Segments.removeSegment()` -> `markSegmentAsUnused()`
 - `Segment.enableSegment()` -> `markSegmentAsUsed()`

</li>
</ul>

<h4>4.4.d. Point to the changed classes and methods key for understanding the PR</h4>

If the PR is big and a lot of mechanical or auxiliarly changes, help reviewers to not miss the important changes by
including text like the following into the description of your PR:
`The key classes added in this PR are MyNewInterface and MyEntry, as well as changes in SomeExistingClass.` 

<h3>4.5. Persist pushing your PRs through, don't abandon them</h3>
Don't abandon your PRs unless you have realized (or it has been uncovered in the discussion of the PR) that it will
have straightaway net negative effect on the value and the quality of the project if merged. Beware of the "commitment
bias" (a.k.a. "sunk-cost fallacy"), but consider that by abandoning a PR you are sinking not only your own efforts but
also the efforts of the reviewers of your PR.

<h3>4.6. Prioritize reviewing PRs which await reviewers' attention for longer, not the freshest PRs</h3>

When choosing a PR to review, all other things being equal, pick those that lack the attention of reviewers for longer
rather than PRs that are just published. On Github, sort issues in the [least recently updated order](
https://github.com/apache/incubator-druid/pulls?q=is%3Apr+is%3Aopen+sort%3Aupdated-asc) and look for those that appear
to be complete, that is, passing CI checks, and where the author have responded the latest comments by reviewers (if
there are any).

Such PRs may conflict with the `master` branch because they we open for long time. In this case, leave a commit like
`@pr-author, could you please resolve conflicts with the master branch?`

<a name="review-withdraw"/>
<h3><a href="#review-withdraw">#</a> 4.7. Once started reviewing a PR, complete your review, or withdraw explicitly</h3>

Don't start reviewing a PR (or just leave FUD comments) and then stop responding. Either follow through to complete your
review of the PR, or withdraw explicitly by posting a comment like
`I withdraw from reviewing this PR. I'm not leaving neither a +1 nor a -1 vote.`

<h3>4.8. Review PRs as quickly as possible</h3>

<a name="review-batches"/>
<h4><a href="#review-batches">#</a> 4.7.a. Avoid "batching" of review comments, but post comments that you already have
when you stop reviewing the PR for a day or longer</h4>

For PRs until a certain size, try to allocate a sufficiently large time block for completing a review at once. However,
if a PR is too large for that, or if you don't have time or sufficiently sharp focus in a moment to complete a review,
post the comments that you already have so that the PR author can start addressing them, [note until what place you
have reviewed the code already](#batch-until-note), and [post the estimated time in what you will be able to continue
the review](#be-explicit).

<h4>4.8.b. Don't wait until the PR author resolves comments by other authors, or comments from your previous review
batch for continuing your review</h4>

Even if you have to split your review in batches, post them and complete your review as quickly as possible regardless
of any other activity (or inactivity) happening in a PR.

<a name="batch-until-note"/>
<h4><a href="#batch-until-note">#</a> 4.8.c. When posting a review batch, note until which file (and line of code) your
have done your line-by-line review</h4>

Leave a comment like `Reviewed until Foo.java, line XXX.`

<h3>4.9. Limit "nice to have" asks in a PR</h3>

When reviewing a PR which doesn't meet your own standard of quality, don't block it until the author does everything
that you would have done if you was preparing such a PR yourself. Instead, cap the amount of requests for code
improvements using the following rules:

 1. If the PR author applies all proposed changes, the size of their PR shouldn't grow more than by 100 lines of changed
 code or by 10% of the original PR's size for large PRs of more than 1000 changed LOC.
 2. Propose "nice to have" improvements only during the first round of review (be it in a single batch or several
 batches). However, if the author amends the PR with new code, the first round of review of the new code can also
 include "nice to have" asks (abiding to the first rule).

Note that comments with concerns about the [high-level design of the solution](#design) and [insufficient documentation
and code comments](#comment-document) are *not* considered "nice to have". Don't hesitate to raise the concern when you
come to realization that the class structure proposed in the PR will be seriously limiting or suboptimal in long term,
for example, even if it happens late in the PR review process.

<h3>4.10. Write self-contained messages</h3>

In all communication in the project (issues, PR descriptions, comments, discussion messages), explain the reasoning
behind your opinions and conclusions. If possible, provide links to resources so that other parties can obtain the same
infromation that you have and verify your line of reasoning.

When reasonable, try to always add "because" clause to your statements:

 - `I don't like this solution because [...] Instead, I think X would be better because ...`
 - `This construction is suboptimal: see https://github.com/code-review-checklists/java-concurrency#...`

<a name="enrich-comments"/>
<h4><a href="#enrich-comments">#</a> 4.10.a. Enrich your comments with context and links to resources</h4>

Don't assume that everybody posesses the same knowledge and context as you do. Link to project's checklists, Wikipedia,
blog posts, other places in the codebase, books, and any other resources which back up your perspective.

<a name="be-explicit"/>
<h4><a href="#be-explicit">#</a> 4.10.b. Be unnaturally explicit in communication</h4>

Explicitly state your assumptions, intents, positions, and plans even if it seems obvious, weird, or mouthful:

 - Identify whether your review is complete or incomplete (as per principle [When posting a review batch, note until
 which file (and line of code) your have done your line-by-line review](#batch-until-note)).
 - Identify the level of your review: [high level](#high-level-review) or [code level](#line-by-line-review). In the
 former case, leave a comment like the following:
 `I didn't review the code in this PR line-by-line. Leaving some high-level comments: ...`
 - Identify whether your review is approval, approval with optional comments (which the PR author should do according to
 principle [Do everything "optional" that reviewers ask you to do](#do-optional), but is free not to do), request of
 changes, or refrain from resolution. The latter should primarily be used when you comment on some small part of the PR
 and not planning to review the whole PR. Prefer using the appropriate buttons in Github's review interface ("Approve"
 when you approve the PR or "Request changes" when you request changes) rather than always using the generic "Comment"
 button.
 - When you [have to split your review between several batches](#review-batches), publish the most likely time when you
 will be able to continue the review: `I will be able to continue the review within several days.` 
 - In a description of a new issue, note if you are planning to work on the issue in the near (or forseeable) future or
 not. Or, if you are a committer, instead of writing `I'm going to work on this issue in the near future`, you can just
 assign the issue to yourself. Instead of writing `I'm not going to work on this issue in the near future`, you can just
 `Contributions Welcome` tag to the issue.
 - More generally, reveal your plans (especially near-term and certain plans) regarding doing any work on the project to
 reduce the probability of a clash with other developers.
 - *An absence of specific information is an information*: be explicit about that your didn't think about something, or
 did do something else, etc. (which is often OK in itself. Examples:
    - `There isn't anything specific about the design of my code: it just seemed reasonable to me when I was working on
    the feature. I didn't consider any alternative designs.`
    - `I didn't benchmark this code. I don't expect performance surprises because the code is straightforward, there are
    no tricky algorithms or potentially expensive calls in it.`
    - `I didn't research this question deeper. With my current level of understanding, I think the proposed solution is
    a low risk.`
 - Be explitit about the things you don't know or *cannot know*: [Explore and expose the uncertainty actively
 ](#uncertainty).

<h4>4.10.c. Write review comments with instructions and answers, not just questions</h4>

  - Don't just ask the PR author something about the code, assuming that they will follow principle [Change the code (or
  add code comments) when a reviewer asks anything about it](#update-code-on-questions) and will update the code or will
  add code comments. Instead, write something like
  `I don't understand this code. Could you make the code more obvious or add a code comment explaining it?`
 - Instead of writing something like `This looks like a bug`, expand to
 `I can't understand why you did this. It looks like it might be incorrect for X reason. Could you please look into
 that, and if it seems like the code is correct after all, add a comment explaining how it works?`

<a name="exact-review-suggestions"/>
<h4><a href="#exact-review-suggestions">#</a> 4.10.d. Propose exact code changes and comments to be added in the
code</h4>

Whenever you have exact picture of how the code should be changed (or what comment should be added), post it. Don't
treat review comments as an educational space where the authors should "get to the right answer themselves". Also don't
worry that by posting exactly how you think the patch should be updated is "micromanaging" PR authors. PR authors still
have freedom to not apply your exact version ([perhaps coming up with something even better](#answer-in-code)), but it's
helpful for them to know your vision precisely. 

<a name="argue-on-disagreement"/>
<h3><a href="#argue-on-disagreement">#</a> 4.11. Don't argue unless you disagree on the point</h3>

Consider the high cost of arguing: back and forth replies may take days or even weeks. Instead, just doing what the
reviewer asks you to do may be the most effective path forward if you are indifferent to what the reviewer proposes, or
you generally agree but think that the change is unimportant.

However, don't silently apply changes which your *disagree* with, even to the slightest degree. Seek to understand why
you disagree with the reviewer and [try to get to the same page with them](#truth).

See also principles [Do everything "optional" that reviewers ask you to do](#do-optional) and [Respect reviewer's
requests](#respect-review).

<a name="answer-in-code"/>
<h4><a href="#answer-in-code">#</a> 4.11.a. Answer in code</h4>

If you do disagree with a reviewer's proposition but recognize that your current code is not optimal either, or you can
agree that some version of the code halfway between your current version and what the reviwer proposes is good, instead
of beginning an argument in comments on Github, *go directly to changing the code in your PR* to the version which is
acceptable for you and with which the reviewer would likely argee and respond to them describing what you have done. 

<h3>4.12. Respond to <i>every</i> review comment</h3>

As a PR author, respond to every review comment left to your PR, describe what action have you taken to address the
comment. Often, it can be just `Fixed` or `Changed`. When your action differs from what the reviewer asked (or they
didn't ask to do anything in particular, but merely pointed to a problem) the response should be more detailed.

<h2>5. Openness</h2>

<a name="explain-yourself"/>
<h3><a href="#explain-yourself">#</a> 5.1. Explain your design choices, decisions, and opinions</h3>

<a name="explain-design"/>
<h4><a href="#explain-design">#</a> 5.1.a. Provide the reasoning behind your designs, decisions, and opinions up to your
focus areas</h4>

Whenever you describe a solution in an issue or in a PR description, or express an opinion about someone else's solution
during review, expose the full line of your reasoning up to your (or your organization's) [focus areas](#focus-areas).
Examples:

 - `We chose this approach over the alternative X because implementing X would lead to Y that, in turn, would negatively
 affect <focus area> of Druid which is a priority for us.`
 - `I don't like the proposed design because it has negative effect on <some quality> of Xxx Druid nodes, while not
 offering much in exchange. I wouldn't approve this design until the necessary research is done and it's shown that
 there is no alternative design which doesn't have such a negative effect on <some quality> while being comparable with
 the proposed design in other aspects.`

<a name="alternatives"/>
<h4><a href="#alternatives">#</a> 5.1.b. Discuss the alternatives to your solutions and designs which you considered and
dismissed</h4>

In a proposal or a PR description, describe the approaches alternative to the proposed or the implemented solution and
compare them in the perspective of the selected focus areas, such as *performance*, *fault tolerance*,
*maintainability*, *ease of cluster operation*, etc.

<h4>5.1.c. In review comments, link to checklist items explaining the problem with the code</h4>

See also principle [Enrich your comments with context and links to resources](#enrich-comments).

<h3>5.2. Don't leave discussions behind your organization's doors</h3>

It may be easy to forget to [explain your design choices and decisions](#explain-yourself) when you discuss the design
within your organization. Outline the alternative solutions in the issue, PR, or comment that is the public result of
(or influenced by) some internal discussions and explain why they were rejected.

If no alternatives were considered and there were no debates about the design within your organization state this too,
according to principle [Be unnaturally explicit in communication](#be-explicit):
`Internally, @my-colleague reviewed this design and found it reasonable. We didn't consider any alternatives.` 

<h4>5.2.a. Do asynchronous code review on Github rather than using some internal medium</h4>

When the workflow of your organization involves asynchronous review of open Druid source code, prefer to conduct such a
review as an ordinary public review on Github rather than doing the review privately and then having the author to
publish the polished version of the code. The assets of any code review can be a learning resource for other developers
outside of your organization.

<a name="probe-reasoning"/>
<h3><a href="#probe-reasoning">#</a> 5.3. Probe other people's reasoning</h3>

If other developers don't [reveal the reasoning](#explain-yourself) behind their designs, decisions, opinions, or review
comments, ask them about it:

 - `Why did you choose this design?`
 - `Did you consider alternative solutions?`
 - `Why do you think the organization of the code that you propose would be better than the current version?`

Note that asking such questions is an exception from principle [Don't ask questions. Give answers](#give-answers)
because you cannot know what is on the minds of other people.

<h3>5.4. Engage in thoughtful disagreement mindfully</h3>

When a disagreement between you and some other developers arises regarding the qualities of some solution or a design,
or the processes of development, or the directions of the project, try to understand why there is a difference in your
opinions by [probing other developers' reasoning](#probe-reasoning) as well as [exposing your own reasoning
](#explain-yourself).

<h4>5.4.a. Express your position assertively</h4>

Openly face the possibility to being proven wrong publicly by expressing your opinion clearly and assertively. This will
help other developers to get to the root of the disagreement quicker and thus will make the communication more
effective.

<h4>5.4.b. Call up neutral committers to resolve the disagreement stemming from the difference in yours and other
people's priorities in Druid development</h4>

If it appears that the disagrement on what is an acceptable solution for a particular problem comes from the fact that
you and other developers value different things more in Druid development or have different goals regarding the future
of Druid, ask some other Druid committers who are not affiliated with either yours or your opponents' organization to
express their opinion as well to break the tie.

See also [the Apache decision making process](https://www.apache.org/foundation/how-it-works.html#decision-making).

<a name="truth"/>
<h4><a href="truth">#</a> 5.4.c. Be willing to get to the truth, not to win the argument</h4>

Be happy to be proven wrong and to get rid of your fallacies if your opponent has a more correct understanding of the
problem.

<h4>5.4.d. Consider your own and your opponents' believability in the area of the debatable question</h4>

Be mindful about your own beievability (expertise) compared with the believability of your opponents on the topic of the
debate. If you are not an expert on some subject and don't have very strong reasons to think that your opinion is right
it might be more practical to resolve the deadlock by proceeding with the design suggested by the developers who have
more (successful) experience of designing solutions in the given problem space.

<h3>5.5. Seek the scrutny of your work</h3>

<h4>5.5.a. Try to get the hardest, not the lightest reviewers to check your work</h4>

In the description of a PR or a proposal, at-mention (like `@somebody`) developers who are

 - The greatest experts on the subject of your work.
 - Most likely to point to weaknesses in your solution.

Don't assume that all Druid developers monitor the newly published PRs and issues at all times and that merely
publishing a PR without explicit mention of a developer is sufficient to ensure that the developer is at least aware of
your PR or issue.

<h4>5.5.b. Don't "sneak" PRs into master by excersizing the power of having more than two Druid committers in your
organization</h4>

The Druid's rule is that a PR without a `Design Review` tag and with only one approval from a committer should remain
open for at least three working days until it's appropriate to merge it unless the PR is of high operational importance
(for example, it fixes the CI build). PRs with two approvals from committers could be merged immediately.

However, if there are two or more Druid committers working in your organization, avoid using this power to quickly merge
things that you want to merge without leaving developers from other organizations a chance to examine your work. Try to
extend the "open lifetime" of a PR towards the same three days when possible. For example, if you are working on a
series of PRs a PR could be kept unmerged (up to three working days) until the moment when the next PR is ready to be
published so that the total time when there is an outstanding open PR in the series where other Druid developers may
chime in (including with general concerns about the series) is greater.

<a name="uncertainity"/>
<h3><a href="#uncertainity">#</a> 5.6. Explore and expose the uncertainty actively</h3>

Principle [Don't ask questions. Give answers](#give-answers) shouldn't be read as that developers should always
communicate "confidently", either communicating definitive information assertively, or no information at all. Quite the
contrary. Be open about what you don't know, don't understand, or cannot estimate or predict well. A few examples:

 - In a proposal:
 `I cannot tell whether this change will improve or worsen the performance of the system before implementing it and
 running benchmarks. It it results in a performance decline within 5%, I would still proceed with this change because it
 enables implementing features X and Y in the future.`
 - `I don't understand and cannot find any clues in the code why this logic is implemented this way currently. It dates
 back to the initial Druid OSS commit. I suggest to change it to the way X because it's better w.r.t. Y and I cannot
 think of any potential drawbacks.
 - `In our experience, the typical values of X are 100..200, so the default limit 500 seems reasonable. However, it may
 be that some Druid users run queries with vastly different values of X, from < 10 to > 1000. In those cases, this
 feature might not work well. For this reason, we are leaving the old way of running queries as a backup option and add
 appropriate notes in the documentation.`

See also principle [Express fear or doubt in a constructive way](#constructive-fud) and [Be unnaturally explicit in
communication](#be-explicit).
 
<h2>6. Learning</h2>

<h3>6.1. Offload your knowledge into project's assets</h3>

Offload your knowledge about the project as well as about the general principles of programming in Java and using
libraries such as Guava into durable and linkable project's assets as much as possible.

<a name="suggest-explaining-comments"/>
<h4><a href="#suggest-explaining-comments">#</a> 6.1.a. Suggest explaining comments during reviews</h4>

If during a code or a design review you find yourself explaining something to the author of the work or to other
developers at length suggest them to add [explaining comments](#explaining-comments) at certain places in the code
instead (according to principle [Propose exact code changes and comments to be added in the code
](#exact-review-suggestions)).

You can also create a PR with these comments yourself if you want to preserve your authorship in the project's commit
log and you don't mind the overhead of creating a separate PR.

<a name="add-checklist-item"/>
<h4><a href="#add-checklist-item">#</a> 6.1.b. Add code review checklist items, one at a time</h4>

If during review you notice yourself (or anyone else) leaving a comment about some problem with the code or the design
that looks like a pattern but can't be easily [automated using any of the static analysis tools
](#open-issues-defects-static-analysis) (in which case you should create an issue about that) create a new checklist
item to some document in `dev/code-review/` directory in the project. If there is no relevant file in the directory yet,
create a new one.

Practically, there are two problems with putting this principle into practice:
 - Adding a new checklist item while you are reviewing or developing code disturbs the working flow.
 - By the time of the day when you are doing code review you may not have sufficient mental vigor to put together a
 general-purpose, well-written checklist item.

The solution to both problems is adding a note for yourself in a personal to-do list to create checklist items and
process them in a dedicated time slot. The remaining thing is to ensure that the to-do list is processed regularly: see
section [Responsibility and leadership](#responsibility-and-leadership).

After creating a PR with a new checklist item, announce it in the development mailing list to both ensure that more
developers become aware of the new checklist item (see also principle [Share your discoveries in the development mailing
list](#share-learning)) and that more developers agree with the wording of the checklist item. They may have different
perspectives and help to make the wording of the checklist item more general and balanced.

<a name="use-checklists"/>
<h3><a href="#use-checklists">#</a> 6.2. Use <a href="code-review">checklists</a> regularly</h3>

It may be impractical to go through all items in all checklists for all PRs at all times, but to ensure that the
knowledge from the checklists is [internalized](#internalize-review-comments) and you mostly apply the checklists
automatically in your mind, regularly refresh all items in all checklists by reading them and trying to apply to real
code.

There may be different strategies of doing this:
 - Select a block of checklist items and go over them for all code that you review during some week (including your own
 code). The next week, move to the next block of checklist items.
 - Review 4 out of 5 PRs (including your own PRs, as per principle [Review your PRs yourself](#self-review)) without
 using checklists and go over all checklist items for every fifth PR that you review.
 - Some mix of the above.

<h3>6.3. Review these principles regularly</h3>

Just like it's impossible to always remember all coding and design principles (descibed in checklists), reading these
principles once is not enough to put them into practice. Read through this principles and [committer's instructions](
committer-instructions.md) every once in a while and recognize which principles do you usually follow yourself and which
principles "need work". You may also find that you don't quite agree with some principles anymore, at least with their
wording. Start a discussion about this.

<h3>6.4. Reflect on the community development process to improve the principles</h4>

Be mindful about the community development process you participate or observe. Look for inefficiencies, injustice, and
repeated patterns. Think how the existing principles could be more accurate, or of some new principles which would
mitigate these problems in the community development process if followed by some participant.

<a name="share-learning"/>
<h3><a href="#share-learning">#</a> 6.5. Share your discoveries in the development mailing list</h3>

When you learn something new about the programming: useful API from one of the libraries that are already used in the
project, effective programming technique or a pattern, or find a good learning resource, share these findings in a
message to the development mailing list.

Think if the thing that you have learned can be turned into [static analysis rules](#automate) or [checklist items
](add-checklist-item).

<h2>7. Respect</h2>

<a name="respect-principles-ignoring"/>
<h3><a href="#respect-principles-ignoring">#</a> 7.1. Respect people's right not to follow these principles</h3>

These principles are not mandatory. An author of a PR ignoring some or all these principles is not a reason to block the
PR in itself, although you may demand some specific things such as adding [documentation](#document-comprehensively) or
[tests](#tests). Regardless of how the work and the communication is conducted, only the merits of the final result
should be considered when deciding whether a PR is good for merge or not.

However, never lower your own standard. [Always follow the principles no matter if other people do that
](#lead-yourself).

<h3>7.2. Don't "ping" people in PRs too often</h3>

If a reviewer hasn't finished review of your PR, don't "poke" them by leaving a comment like
`@reviewer do you have more comments?` sooner than in one week since their last activity in the PR and don't poke them
more frequently than one time per week if they don't react to your previous comments.

Don't assume that reviewers can forget about their committments and that you should "help" them by pinging, or that your
pings will change the priority of your PR in the list of things they have to pay attention to.

If a reviewer remains silent, the PR can be merged (if there are enough approvals and all existing comments by the
unresponsive reviewer are thought to be addressed reasonably) no sooner than in two weeks since their last activity in a
PR.

<h4>7.2.a. Don't ping people repetitively if they don't react to your request for review</h4>

If you want someone to review your PR and leave a comment like `@someone could you please review this PR?` but this
person doesn't respond, don't poke theim again. Assume that they are not interested or can't review your PR.

<a name="be-polite"/>
<h3><a href="#be-polite">#</a> 7.3. Be polite</h3>

 - When leaving review comments asking PR authors to do something, prefer polite forms such as `Could you please do X?`
 or `Please do X` to `Do X`.
 - Prefer using modal verbs *could* and *would* to *can* and *will* respectively. See examples in principles [Express
 fear or doubt in a constructive way](#constructive-fud) and [Provide the reasoning behind your designs, decisions, and
 opinions up to your focus areas](#explain-design).
 - Add "I think" or "In my opinion, " (or "IMO, ") to sentences that indeed express your opinion rather than facts.

<h3>7.4. Be thankful</h3>

Thank people for their PRs, reviews, or other work. The benefits of gratitude probably outweigh the negative effect of
destraction created by extra comments in issues and PRs which you wouldn't leave otherwise. However, when possible, try
to amend words of gratitude to comments with some other information that you are going to post anyway.

<a name="respect-review"/>
<h3><a href="#respect-review">#</a> 7.5. Respect reviewer's requests</h3>

As a PR author, accept reviewers' comments and satisfy their requests without questioning the optionality or
practicality of the comments to some reasonable limit. Focus on getting a PR to the state which all people agree with as
quickly as possible rather than showing reviewers the "right" way to make a review.

See also principles [Do everything "optional" that reviewers ask you to do](#do-optional) and [Don't argue unless you
disagree on the point](#argue-on-disagreement).

<h3>7.6. Treat your own review comments as optional when PR authors disagree with them</h3>

If the PR author pushes back on the changes that you propose, consider if these comments are true blockers for the PR.
If they are not, don't insist on addressing your comments and approve the PR. 
