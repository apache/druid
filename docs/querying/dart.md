---
id: dart
title: "Dart query engine"
sidebar_label: "Dart"
description: Use the Dart query engine for light-weight queries that don't need all the capabilities of the MSQ task engine.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info[Experimental]

Dart is experimental. For production use, we recommend using the other available query engines.

:::


Use the Dart query engine for light-weight queries that don't need all the capabilities of the MSQ task engine. For example, a good query to use for Dart is a GROUP BY query that has intermediate results consisting of hundreds of millions of rows. Dart is able to compute these types of queries quickly because its multi-threaded workers perform in-memory shuffles using locally cached data. 

You can query batch or realtime datasources with Dart.

## Enable Dart

To enable Dart, add the following line to your `broker/runtime.properties` and `historical/runtime.properties` files:

```
druid.msq.dart.enabled = true
```

### Additional configs

There are additional configs that provide some control over Dart's resource consumption.

For Brokers, you can set the following configs:

- `druid.msq.dart.controller.concurrentQueries`: The maximum number of query controllers that can run concurrently on that Broker. Additional controllers are queued. Defaults to 1.
- `druid.msq.dart.query.context.targetPartitionsPerWorker`: The number of partitions per worker to create during a shuffle. We recommend setting this to the number of threads available on workers to fully take advantage of multi-threaded processing of shuffled data.

For Historicals, you can set the following configs:

- `druid.msq.dart.worker.concurrentQueries`: The maximum number of query workers that can run concurrently on that Historical. Default is equal to the number of merge buffers because each query needs one merge buffer. Ideally, this should be equal to or larger than the sum of the `concurrentQueries` setting on yourl Brokers.
- `druid.msq.dart.worker.heapFraction`: The maximum amount of heap available for use across all Dart queries as a decimal. The default is 0.35, 35% of heap.


## Run a Dart query

Once enabled, you can select Dart from the available engines in the Druid console or the API to issue queries like with other query engines.

### Druid console

In the **Query** view, select **Engine: SQL (Dart)** from the engine selector menu.

### API

Dart uses the SQL endpoint `/druid/v2/sql` like the other SQL query engines. To use Dart, include the query context parameter `engine` and set it to `msq-dart`:

<Tabs>
  <TabItem value="SET" label="SET" default>
    
  ```sql
  curl --location 'http://HOST:PORT/druid/v2/sql' \
--header 'Content-Type: application/json' \
--data '{
  "query": "SET engine = '\''msq-dart'\'';\nSELECT\n  user,\n  isRobot\nFROM wikipedia",
  ...
  ...
}'
  ```

  </TabItem>
  <TabItem value="context_block" label="Context block">
    
  ```sql
  curl --location 'http://HOST:PORT/druid/v2/sql' \
  --header 'Content-Type: application/json' \
  --data '{
  "query": "SELECT\n  user,\n  isRobot\nFROM wikipedia",
  ...
  ...
  "context": {
    "engine":"msq-dart"
    ...
  }
  }'
  ```

  </TabItem>
  </Tabs>