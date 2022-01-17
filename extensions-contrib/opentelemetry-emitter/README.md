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

# OpenTelemetry Emitter

The [OpenTelemetry](https://opentelemetry.io/) emitter generates OpenTelemetry Spans for queries.

## How OpenTelemetry emitter works

The [OpenTelemetry](https://opentelemetry.io/) emitter processes `ServiceMetricEvent` events for the `query/time`
metric. It extracts OpenTelemetry context from
the [query context](https://druid.apache.org/docs/latest/querying/query-context.html). To link druid spans to parent
traces, the query context should contain at least `traceparent` key.
See [context propagation](https://www.w3.org/TR/trace-context/) for more information. If no `traceparent` key is
provided, then spans are created without `parentTraceId` and are not linked to the parent span. In addition, the emitter
also adds other druid context entries to the span attributes.

## Configuration

### Enabling

To enable the OpenTelemetry emitter, add the extension and enable the emitter in `common.runtime.properties`.

Load the plugin:

```
druid.extensions.loadList=[..., "opentelemetry-emitter"]
```

Then there are 2 options:

* You want to use only `opentelemetry-emitter`

```
druid.emitter=opentelemetry
```

* You want to use `opentelemetry-emitter` with other emitters

```
druid.emitter=composing
druid.emitter.composing.emitters=[..., "opentelemetry"]
```

_*More about Druid configuration [here](https://druid.apache.org/docs/latest/configuration/index.html)._

## Testing

### Part 1: Run zipkin and otel-collector

Create `docker-compose.yaml` in your working dir:

```
version: "2"
services:

  zipkin-all-in-one:
    image: openzipkin/zipkin:latest
    ports:
      - "9411:9411"

  otel-collector:
    image: otel/opentelemetry-collector:latest
    command: ["--config=otel-local-config.yaml", "${OTELCOL_ARGS}"]
    volumes:
      - ${PWD}/config.yaml:/otel-local-config.yaml
    ports:
      - "4317:4317"
```

Create `config.yaml` file with configuration for otel-collector:

```
version: "2"
receivers:
receivers:
  otlp:
    protocols:
      grpc:

exporters:
  zipkin:
    endpoint: "http://zipkin-all-in-one:9411/api/v2/spans"
    format: proto

  logging:

processors:
  batch:

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging, zipkin]
```

*_How to configure otel-collector you can read [here](https://opentelemetry.io/docs/collector/configuration/)._

Run otel-collector and zipkin.

```
docker-compose up
```

### Part 2: Run Druid

Build Druid:

```
mvn clean install -Pdist
tar -C /tmp -xf distribution/target/apache-druid-0.21.0-bin.tar.gz
cd /tmp/apache-druid-0.21.0
```

Edit `conf/druid/single-server/micro-quickstart/_common/common.runtime.properties` to enable the emitter (
see `Configuration` section above).

Start the quickstart with the apppropriate environment variables for opentelemetry autoconfiguration:

```
OTEL_SERVICE_NAME="org.apache.druid" OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317" bin/start-micro-quickstart
```

*_More about opentelemetry
autoconfiguration [here](https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure)_

Load sample data - [example](https://druid.apache.org/docs/latest/tutorials/index.html#step-4-load-data).

### Part 3: Send queries

Create `query.json`:

```
{
   "query":"SELECT COUNT(*) as total FROM wiki WHERE countryName IS NOT NULL",
   "context":{
      "traceparent":"00-54ef39243e3feb12072e0f8a74c1d55a-ad6d5b581d7c29c1-01"
   }
}
```

Send query:

```
curl -XPOST -H'Content-Type: application/json' http://localhost:8888/druid/v2/sql/ -d @query.json
```

Then open `http://localhost:9411/zipkin/` and you can see there your spans.