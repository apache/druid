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

# OpenTelemetry Extensions

The [OpenTelemetry](https://opentelemetry.io/) extensions provides the ability to read metrics in OTLP format

## Configuration

### How to enable and use the extension

To enable the OpenTelemetry Metrics extensions, add the extension and enable the extension in `common.runtime.properties`.

Load the plugin:

```properties
druid.extensions.loadList=[..., "druid-opentelemetry-metrics-extensions"]
```

Now Sumbit the Supervisor Config with [source input format](https://druid.apache.org/docs/latest/ingestion/data-formats/) as:

```
"ioConfig": {
  "topic": "topic_name",
  "inputFormat": {
    "type": "opentelemetry-metrics-protobuf",
    "metricDimension": "name"
  }
  .
  .
  .
}
```

## Supported Types
### Metric Types:
* SUM
* GAUGE

### Data Types:
* INT_VALUE:
* BOOL_VALUE:
* DOUBLE_VALUE:
* STRING_VALUE:
