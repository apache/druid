/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextConfig;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DartSqlEngineTest
{
  private static final ServerConfig SERVER_CONFIG = new ServerConfig();

  @Test
  public void testInitContextAppliesDartDefaults()
  {
    final DartSqlEngine engine = makeEngine(Map.of("key", "dart-default", "key2", "dart-default2"));
    final Map<String, Object> userContext = new HashMap<>();
    userContext.put("key", "user-value");
    userContext.put("key2", null);

    final QueryContext context = engine.initContext(QueryContext.of(userContext));

    // A user value wins over the Dart default, and a null user value falls back to it
    Assertions.assertEquals("user-value", context.get("key"));
    Assertions.assertEquals("dart-default2", context.get("key2"));
    Assertions.assertEquals(
        SERVER_CONFIG.getDefaultQueryTimeout(),
        context.get(QueryContextParameters.TIMEOUT.getName())
    );
    Assertions.assertNotNull(context.get(QueryContextParameters.DART_QUERY_ID));
  }

  @Test
  public void testInitContextKeepsUserTimeoutAndReplacesDartQueryId()
  {
    final DartSqlEngine engine = makeEngine(Map.of());
    final QueryContext userContext = QueryContext.of(
        Map.of(
            QueryContextParameters.TIMEOUT.getName(), 1234L,
            QueryContextParameters.DART_QUERY_ID.getName(), "user-dart-query-id"
        )
    );

    final QueryContext context = engine.initContext(userContext);

    Assertions.assertEquals(1234L, context.get(QueryContextParameters.TIMEOUT.getName()));
    // The Dart query ID must be globally unique, so a user-provided one is replaced
    Assertions.assertNotEquals("user-dart-query-id", context.get(QueryContextParameters.DART_QUERY_ID));
  }

  @Test
  public void testInvalidDefaultContextNamesDartProperty()
  {
    final IAE e = Assertions.assertThrows(
        IAE.class,
        () -> makeEngine(Map.of("noSuchParameter", 1), new QueryContextConfig(true))
    );
    Assertions.assertEquals(
        "Invalid default query context [druid.msq.dart.query.context]: "
        + "Unknown query context parameter [noSuchParameter].",
        e.getMessage()
    );
  }

  @Test
  public void testInvalidDefaultContextNamesGivenProperty()
  {
    final IAE e = Assertions.assertThrows(
        IAE.class,
        () -> new DartSqlEngine(
            null,
            null,
            null,
            null,
            null,
            null,
            SERVER_CONFIG,
            new DefaultQueryConfig(Map.of("noSuchParameter", 1)),
            null,
            null,
            new QueryContextConfig(true),
            "druid.other.query.context"
        )
    );
    Assertions.assertEquals(
        "Invalid default query context [druid.other.query.context]: "
        + "Unknown query context parameter [noSuchParameter].",
        e.getMessage()
    );
  }

  private static DartSqlEngine makeEngine(final Map<String, Object> dartDefaults)
  {
    return makeEngine(dartDefaults, QueryContextConfig.DEFAULT);
  }

  private static DartSqlEngine makeEngine(
      final Map<String, Object> dartDefaults,
      final QueryContextConfig queryContextConfig
  )
  {
    return new DartSqlEngine(
        null,
        null,
        null,
        null,
        null,
        null,
        SERVER_CONFIG,
        new DefaultQueryConfig(dartDefaults),
        null,
        null,
        queryContextConfig
    );
  }
}
