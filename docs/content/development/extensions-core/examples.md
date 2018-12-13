---
layout: doc_page
title: "Extension Examples"
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

# Extension Examples

## TwitterSpritzerFirehose

This firehose connects directly to the twitter spritzer data stream.

Sample spec:

```json
"firehose" : {
    "type" : "twitzer",
    "maxEventCount": -1,
    "maxRunMinutes": 0
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "twitzer"|N/A|yes|
|maxEventCount|max events to receive, -1 is infinite, 0 means nothing is delivered; use this to prevent infinite space consumption or to prevent getting throttled at an inconvenient time.|N/A|yes|
|maxRunMinutes|maximum number of minutes to fetch Twitter events.  Use this to prevent getting throttled at an inconvenient time. If zero or less, no time limit for run.|N/A|yes|
