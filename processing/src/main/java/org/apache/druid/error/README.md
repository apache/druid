# Guide to Druid Error Messages

Errors in Druid are complex. Errors come from both Druid code and from libraries.
The audience for errors varies depending on the type of error. Managed systems
often want to redact sensitive information, while developers need full details.
Each subsystem within Druid has evolve to use its own error handling methodology.
The goal of this note is to explain how we wish to handle errors moving forward.

## Requirements

Druid has a robust set of error handling requirements that, taken together drive
the error handling implementation explained below.

### Audiences

Errors must address a number of audiences. While most errors are returned to the
end user, it may sometimes be the case that the end user can’t fix the issues.
For example, the user can fix errors in a SQL statement, but cannot fix
configuration issues. Still, the user is always the first point of contact
for errors.

When the error is something that the user cannot fix, we must strike a balance:
provide the user enough information to request the proper help, but not to
reveal internal details that the user does not need, or that the user should
not see. At the same time, if someone other than the user has to fix the error
(a system administrator, a developer, etc.), then that second person does need
the details.

This split audience drives a large part of the error handling system design.

### Managed Deployments

Druid runs in all sizes of deployments. Developers run Druid on a single machine,
and must fix issues arising from code changes. Developers, presumably, have a
strong understanding of Druid internals. New users will download Druid to run on
a laptop, but such users have little knowledge of Druid — they are just getting
started. As Druid moves into production, a TechOps team may run Druid, while a
different set of users load data and issue queries. In a fully managed environment,
users are in different organizations than the people who manage Druid.

This complexity makes clear that the audiences above may not have any contact
with one another. Further, a fully managed environment wants to restrict the
information that “leaks” to the end user. While a developer wants the details,
a user in another company should see a “sanitized” message stripped of internal
details.

This requirement means that Druid errors must be flexible. It should be possible
to both log the details (for the people running Druid), while meaningfully redact
sensitive information for end users in remote organizations.

### Error Categorization

To help with the above requirements, we need to categorize errors. Druid is
complex: there are hundreds (if not thousands) of things that could go wrong,
ranging from bad user input to incorrect configuration, a badly configured network,
or buggy code. We’ve noted that we want to identify the audience for an error.
Sometimes that is easy: the user is responsible for the text of a SQL message.
Other times, it is hard: who is responsible for a network timeout?

We’ve also noted that managed environments want to redact information. Doing so
error-by-error is an impossible task. Doing so by category is more practical.

Druid has the concept of an “error code” which is one level of abstraction above
the detailed error message. We wish to generalize that concept to categorize all
errors. Creating the categories is a non-trivial task: it requires balancing the
audience (when known) with the functional area. For example both timeouts and bad
configuration might be of interest to an “admin”. In larger shops, the Druid admin
is distinct from the network admin. Thus, it might make sense to have a “network”
category distinct from a “config” category. And so on.

Each category may need to include unique information. SQL errors should include
the line number of the error. I/O errors the identify of the resource that failed.
Config errors the name of the config variable. And so on.

Error sanitization systems may use the category to aid in redacting information.
“User” errors might be provided as-is, while “system” errors might be redacted to
a generic “internal error: please contact support for assistance.” In such cases,
the log file would provide “support” with the details.

### Error Destinations

The above sections hint that errors flow to multiple destinations. The end user
(via an API response) is the most obvious destination. Errors also flow to logs,
and from there to many forms of log aggregation systems. As noted, each destination
may receive a different “view” of the error. End users get a simplified, user-focused
view. Developers get the full details, including stack trace. Administrators may
get just “important” errors, and just the description, shorn of stack trace. And
so on.

Druid primarily uses a REST API for its messages. However, each Druid subsystem
has evolved its own way to return errors. The query endpoints use the
`QueryException` format, other endpoints use a variety of ad-hoc formats:
some use plain text, others use ad-hoc JSON, etc.

As Druid evolves, we have added multiple query API protocols: JDBC, gRPC,
etc. Each protocol has its own way to format errors, often not as a REST response.

This means that Druid exceptions don’t have just one format: they must allow
each destination to apply a format appropriate for that destination.

### Forwarding Remote Errors

Druid is a distributed system. Queries run across tiers. The “live” query
system uses scatter/gather in which queries run on data nodes. MSQ runs
across multiple stages. In these cases, a remote node may raise an error
which must be returned to a different node, and then forwarded to the user.
Care must be taken to preserve the error as created on the remote node. In
particular, stack traces should be from the remote node, not the receiving node.

Errors are sent across a REST API. As such, Druid exceptions must be
serializable, in some form which allows recovering the exception on the
receiver (Broker, MSQ controller) side.

By contrast, when errors are returned to the end user, we do not expect that
the user will deserialize the errors using Druid’s error classes. Most clients
don’t have visibility to Druid’s code. Thus, errors returned to the user
should have a standard format so that a single client-side class can
deserialize any Druid exception.

## Implementation

With the requirements out of the way, we can now discuss the implementation
that meets these requirements.

### `DruidException` and its Subclasses

Most exceptions raised within Druid code should use a subclass of the
`DruidException` class. Use this class when the error is to be returned to the
user (and, perhaps, logged.) Use other exceptions when the goal is to throw an
exception caught and handled by some other bit of code, and which is not
returned to the user.

Create a subclass of `DruidException`:

* For each error category.
* Within a category when the error must contain specific additional fields.
* When the class name, when in logs, provides useful information to developers.

These rules provide a three-level hierarchy:

```text
DruidException
  <Category>Exception
    <SpecialCaseForCategory>Exception
```

### Special Fields

Errors include a number of specialized fields that assist with the requirements
above.

* `host`: When an error occurs on a data node, this field indicates the
  identity of that node. When the error occurs on the node that received the
  user response (e.g. the Broker), the field is `null`.
* `suggestion`: Provides suggested corrective action, which may only be valid
  in the case of a simple Druid deployment. For example, “Increase the value
  of the druid.something.memory config variable.” Managed systems may omit
  this text.
* `code`: An error code that identifies the category of error. Categories are
  grouped by target audience: some are for the user (SQL syntax, SQL validation,
  etc.) Some are for the admin (OOM, resource issues.) Some are ambiguous
  (network timeouts.) The code allows managed systems to do wholesale redactions.

### Context

The context is the “get out of jail free” card. The context allows us to add as
much detail to an error as wanted, without running the risk of exposing
sensitive information. In a managed system, the context may be hidden from the
user, but still logged. In a development system, the context gives the developer
the information needed to identify a problem.

Context should include secondary information the can safely be hidden. Primary
information (such as the name of the column that can’t be found) should be in
the message itself.

### Query Endpoint Errors

Errors returned from `/sql` or `/sql/task` have a format defined by `QueryException`:

```json
{
  "error": "<error code>",
  "errorClass": "<class name of the exception>",
  "errorMessage": "<getMessage() value of the exception>",
  "host": "<host on which the error occurred>"
}
```

The `host` is set only for errors that occured on data nodes, but not when the error
occurred on the Broker (in, say, SQL validation.)

The `error` is an ad-hoc set of codes defined in `QueryException`, but is neither
exaustive or unique: some errors could fall into multiple error codes.

Per Druid's compatibility rules, we can add new fields to the above format, but we
cannot remove existing fields or change their meaning. This is particularly unfortunate
for the `errorClass` field since it exposes the specific class name used to throw the
exception: something that will change with the `DruidException` system.

### Data Node Errors

Data nodes (Historical, Peon, Indexer) use the same format as the query endpoint. Such
errors are deserialized into the `QueryException` class. Thus, `QueryException` has a
JSON serialization coupled tightly to both our internal Broker-to-data-node API, and the
external `/sql` API. `DruidException` must fit into this internal API without causing
compatibility issues during rolling upgrades. That is, the wire format must not change.
Short-term, this may mean that we discard information when returning errors from the
data node so that the Broker does not fail due to unexpected fields. This restriction
limits our freedom in crafting good error messages on data nodes.

### MSQ Errors

MSQ introduced a well-organized system to report errors from MSQ tasks to the user by
way of an Overlord task report. The system is unique to the MSQ environment and is not
general enough to handle non-MSQ cases. We do not want to modify the MSQ system. Instead,
we want to ensure that the `DruidException` plays well with the MSQ system.

#### Quick Overview of the MSQ Fault System

A quick review of the MSQ code suggests that `DruidException` tries to solve
the same problems as the MSQ error system, though in perhaps a more general way.

MSQ apparently has a fault system separate from exceptions. MSQ splits errors
into two parts: `MSQFault`, which is JSON-serializable, and `MSQException`, which
is not. There are many subclasses of `MSQFault` which are not also subclasses of
exceptions.

`MSQFault` is part of the `MSQErrorReport`. It seems that `MSQFault` is designed
to capture the fields that are JSON serialized into reports, while `MSQException`
is something that can unwind the stack. This split means we don't have to add
JSON serialization to our exception classes. This is wise since, as we'll see
later, the REST JSON format differs a bit from the MSQ format. A single class
cannot have to distinct JSON serializations. By creating the fault class, MSQ
can control its own specialized JSON format.

Now, let's compare the MSQ system with the proposed Druid exception system.

The `MSQFault` interface has many subclasses: apparently one for each kind of
error. Each class includes fields for any error-specific context. JSON
serialization places those fields as top-level context in the serialized
error. Example:

```json
{
  "errorCode": "<code>",
  "errorMessage": "<msg>",
  "<fault-specific-key>": "<value>, ...
}
```

#### Integration with `DruidException`

In an MSQ task, the MSQ system is reponsible for returnining errors to the
user by way of the Overlord task report. Unlike the "classic" query system
(and unlike other REST service), MSQ does not directly return an error to
the user. Instead, MSQ adds the error to the report, then does substantial
work to shut down workers, wrap up the Overlord task, etc.

As a result, we never expect that an `MSQFault` will need to map to a
`DruidException`. We do, however, expect the need to go the other way. MSQ
reuses substantial portions of the native query codebase. That code doesn't
know if it is running in a historical (where it would just throw a
`DruidException` and exit) or in MSQ (where it has to play well in the MSQ
system.) So, we allow that code to use the `DruidException` system. It is up
to the MSQ worker to translate the `DruidException` into a suitable `MSQFault`,
which is then handled via the MSQ error system.

For example, a `DruidException` may provide fields such as the S3 bucket on
which an I/O error occurred. MSQ can map that to a matching `MSQFault` so that
the information appears as a field in the JSON message.

### REST Endpoint Errors

A section above discussed the form of errors from the `/sql` endpoint. Druid has
hundreds of other REST endpoints, with many ad-hoc error solutions. We propose to
unify error reporting to use the (enhanced) `/sql` format. That is, we use the
`DruidException` everywhere in our code, and we map those exceptions to REST
responses the same way for every REST API (unless there is some special reason
not to.)

### Third-Party Exceptions

Druid uses libraries (including the Java library) that throws its own exceptions.
The only workable approach is:

* Catch such exceptions as close to the cause as possible, then translate the error
  to a `DruidException`, providing Druid-specific context. The original exception is
  attached as a cause, so developers can track down the underlying issue.
* Provide a Servlet filter that catches "stray" exceptions and returns a generic error
  message. These cases indicate gaps in the code where we failed to properly catch and
  handle and exception. Managed systems can't know if the non-Druid exception contains
  sensitive information. So, report the error as something like "An internal error
  occurred. See the logs for details", associated with a unique error category so that
  manage services can replace the wording.

### Other APIs

Druid occasionally offers non-REST APIs: JDBC, gRPC, etc. For these cases, an API-specific
mapping from the `DruidException` to the specialized API can handle the needs of that API.

In an ideal world, `DruidException` would be independent of all APIs, and the REST API
would do its own mapping. Howeer, since REST is standard in Druid, we allow `DruidException`
to serialize to and from Druid's REST API JSON.

### JSON Deserialization

The implementation envisions a large number of `DruidException` subclasses: one per
category, with finer grain subclasses. The JSON format given above was designed based
on a single class: `QueryException`. There is no `type` field that Jackson could use to
recover the subclass.

When running a query, the data node with raise a specific error class, then serialize it
in the generic format. The Broker does not have sufficient information to recover the
original class. Instead, the Broker deserializes exceptions as a `GenericException` class.
The result can be thrown, and will re-serialize to the same format as that sent by the
data node, but it loses the ability to parse exceptions based on the exception class.

### Mutable Exception Fields

Druid prefers that class fields be immutable because such an approach reduces risk in
a multi-threaded system. In an ideal world, `DruidException` fields would also be
immutable, with a "builder" class to gather values. Such a solution is workable only
if we have one (or a very few) exception classes. The design here, however, follows
MSQ practice and envisions many such classes. Creating a builder per class would be
tedious.

Instead, we allow certain `DruidException` fields to be mutable so that they can be
set after the exception is created. Mutable fields include:

* `host` (set at the top level of the data node)
* `context` (to allow callers to add information to an exception as it bubbles up
  the call stack).
* `suggestion` (to allow a higher-level of code to offer a suggestion when the
  code that throws the exception doesn't have sufficient context.)

## Guidelines

With the above requirements and design in mind, we can identify some guidelines
when developers write error messages.

### Wording

Word error messages so that they speak to the end user who will receive the
error as a response to a request. When the error is a “user error” this is
simple. Explain the problem in user terms. That is, rather than “key not found”,
say something like “If you set the X context parameter, you must also set the Y
parameter.”

The task is harder when the error is one only an admin or developer can solve.
We want to provide the information that audience needs to solve the problem.
But, we must provide that information only in logs. For example, if we hit an
assertion error, there is not much the user can do. But, a developer wants to
know where the error triggered and why. For this, provide a generic message to
the user “Druid internal error.” Druid might add “See logs for details.” A
managed service would say, “Contact support.” However, the log should provide
the full details, including the stack trace, and the value that caused the issue.

Thus, some errors must be constructed with two sets of information: the bland
user message, and the details for developers. Use the context to help.

### Interpolated Values

Errors will often include interpolated values. Example: “Table <table> not
found”. Druid has a long-standing convention of always enclosing interpolated
values in square brackets: “Table [foo] not found.” While this format is not
standard English, it is standard Druid, and all error messages must follow
this form.

### Sensitive Values

Error messages want to be as helpful as possible by providing all the details
that would be needed to resolve the issue. This is ideal during development,
or in a single-machine deployment. But, in doing so, a message may leak
sensitive information when run in a managed service.

Errors should thus divide information into two “pools.” The error message itself
should contain only that information which is suitable for a managed service
user. Information which might be considered sensitive should reside in the
context. A managed service can strip context values that a user cannot see.

Another alternative is for a manages service to redact an entire category of
errors, as noted above. Thus, errors should be assigned categories that enable
efficient redaction policies.
