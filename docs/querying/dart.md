---
id: dart
title: "Dart query engine"
sidebar_label: "Dart"
description: Use the Dart query engine for light-weight queries that don't need all the capabilities of the MSQ task engine
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info[Experimental]

Dart is experimental. For production use, we recommend using the other SQL query engines.

:::


Use the Dart query engine for light-weight queries that don't need all the capabilities of the MSQ task engine. For example, a good query to use for Dart is a GROUP BY query that has intermediate results consisting of hundreds of millions of rows. Dart is able to compute these types of queries quickly because its multi-threaded workers perform in-memory shuffles using locally cached data. 

You can query batch or realtime datasources with Dart.

## Enable Dart

In your `broker/runtime.properties` file, add the following line:

```
druid.msq.dart.enabled = true
```

### Optional configs



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