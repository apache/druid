---
id: human-readable-duration
title: "Human-readable Duration Configuration Reference"
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

This page documents human-readable durations used by supported Druid timeout settings.

## Supported formats

Druid accepts three duration representations:

1. A non-negative integer representing milliseconds, such as `30000`.
2. A non-negative integer followed by a lowercase unit, such as `30s`, `5m`, or `2h`.
3. A fixed-length ISO-8601 duration, such as `PT30S`, `PT5M`, or `P1W`.

For example:

```properties
druid.global.http.readTimeout=30s
druid.global.http.unusedConnectionTimeout=5m
druid.global.http.connectTimeout=PT10S
druid.server.http.defaultQueryTimeout=30000
```

## Supported unit suffixes

| Suffix | Unit |
|---|---|
| `ns` | Nanoseconds |
| `us` | Microseconds |
| `ms` | Milliseconds |
| `s` | Seconds |
| `m` | Minutes |
| `h` | Hours |
| `d` | Days |

Suffixes are lowercase and must follow an integer directly. For example, `5m` is valid, while `5 m` and `1.5h` are not supported.

## ISO-8601 durations

Supported fixed-length ISO-8601 values include:

| Value | Meaning |
|---|---|
| `PT30S` | 30 seconds |
| `PT5M` | 5 minutes |
| `PT2H` | 2 hours |
| `P1D` | 1 day |
| `P1W` | 7 days |
| `P1W2D` | 9 days |
| `PT1.5S` | 1.5 seconds |

ISO weeks represent fixed seven-day intervals. Calendar-based periods containing months or years, such as `P1M` or `P1Y`, are not supported.

## Precision and compatibility

Human-readable durations support nanosecond resolution. When a supported timeout setting requires milliseconds, positive sub-millisecond values are rounded up to the next millisecond.

For example:

```properties
druid.global.http.clientConnectTimeout=250us
```

This configuration produces a timeout of one millisecond.

The duration parser rejects negative values and durations exceeding its nanosecond-backed long range.

Existing millisecond-backed settings retain their documented legacy numeric behavior. In particular, `clientConnectTimeout`, `defaultQueryTimeout`, and `maxQueryTimeout` preserve their full non-negative long millisecond range for numeric values, including `Long.MAX_VALUE`, where permitted by validation.
