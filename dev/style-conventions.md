---
id: style-conventions
title: "Druid Style Guide"
sidebar_label: "Druid Style Guide"
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

This document attempts to describe the style that Druid code is expected to follow.

A large amount of the style conventions are handled through IDE configuration and automated checkstyle rules.
 
- For Intellij you can import our code style settings xml: [`druid_intellij_formatting.xml`](
  https://github.com/apache/druid/raw/master/dev/druid_intellij_formatting.xml).
- For Eclipse you can import our code style settings xml: [`eclipse_formatting.xml`](
  https://github.com/apache/druid/raw/master/dev/eclipse_formatting.xml).

While this page might discuss conventions that are also enforced via said mechanisms, the primary intent is to
discuss style-related convention that cannot be (or are extremely difficult to be) enforced through such automated
mechanisms.

## Message Formatting (Logs and Exceptions)

The way that log and exception messages get formatted is an important part of a project.  Specifically, it is
important that there is consistency in formatting such that someone can easily identify and interpret messages.
This consistency applies to both log *and* exception messages.

1. Messages should have something interpolated into them.  Generally speaking, if the time is being taken to generate a message, it is usually valuable to interpolate something from the context into that message.  There are exceptions to this, but all messages should start with the assumption that something should be interpolated and try to figure out what that is.
   * Messages INFO level or above (this includes all Exceptions) cannot leak secrets or the content of data.  When choosing what to interpolate, it is important to make sure that what is being added is not going to leak secrets or the contents of data.  For example, a query with a malformed filter should provide an indication that the filter was malformed and which filter it is, but it cannot include the values being filtered for as that risks leaking data.
2. Interpolated values should always be encased in a `[]` and come after a noun that describes what is being interpolated.  This is to ensure that enough context on what is happening exists and to clearly demark that an interpolation has occurred.  Additionally, this identifies the start and end of the interpolation, which is important because messages that attempt to mimic natural prose that also include interpolation can sometimes mask glaring problems (like the inclusion of a space).
   * Bad: `log.info("%s %s cannot handle %s", "null", "is not null", "INTEGER")` -> `"null is not null cannot handle INTEGER"`
   * Better, but still not wonderful: `log.info("column[%s] filter[%s] cannot handle type[%s]", "null", "is not null", "INTEGER")` -> `"column[null] filter[is not null] cannot handle type[INTEGER]"`
   * Good: `log.info("Filter[%s] on column[%s] cannot be applied to type[%s]`, "is not null", "null", "INTEGER")` -> `"Filter[is not null] on column[null] cannot be applied to type[INTEGER]"`
   * But, if I interpolate an array, I get double `[]`.  Yes, that is true, that is okay.  Said another way, if a log message contains `[[]]` that is indicative of interpolating an array-like structure.
