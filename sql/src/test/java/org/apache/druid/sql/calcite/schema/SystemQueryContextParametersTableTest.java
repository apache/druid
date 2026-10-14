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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.client.BrokerViewOfBrokerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SystemQueryContextParametersTableTest
{
  private final SystemQueryContextParametersTable table = createTable(
      new DefaultQueryConfig(null),
      CalciteTests.SUPER_USER_AUTH_RESULT
  );

  @Test
  public void testExcludesSystemParameters()
  {
    final Map<String, Object[]> rows = scan();
    for (final QueryContextParameter<?> parameter : QueryContextParameters.ALL.get().values()) {
      Assertions.assertEquals(
          parameter.getVisibility() != QueryContextParameter.Visibility.SYSTEM,
          rows.containsKey(parameter.getName()),
          parameter.getName()
      );
    }
  }

  @Test
  public void testEffectiveDefaultRequiresConfigReadAccess()
  {
    final DefaultQueryConfig runtimeConfig = new DefaultQueryConfig(Map.of("timeout", 30000));
    final Map<String, Object[]> rows = scan(createTable(runtimeConfig, CalciteTests.REGULAR_USER_AUTH_RESULT));

    // The catalog is still visible
    Assertions.assertEquals(
        rows.keySet(),
        scan().keySet()
    );
    Assertions.assertEquals("LONG", value(rows.get(QueryContextParameters.TIMEOUT.getName()), "type"));
    // The effective default is hidden the same way whether it is configured, built in, or not set at all
    assertEffective(rows, QueryContextParameters.TIMEOUT, null, null, "UNAUTHORIZED");
    assertEffective(rows, QueryContextParameters.PRIORITY, "0", null, "UNAUTHORIZED");
    assertEffective(rows, QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING, null, null, "UNAUTHORIZED");
  }

  @Test
  public void testEnumWithCustomJsonForm()
  {
    // Shown in the JSON form that queries accept, not as the enum constant name
    final Object[] row = scan().get(QueryContextParameters.CLONE_QUERY_MODE.getName());
    Assertions.assertEquals("excludeClones", value(row, "default_value"));
  }

  @Test
  public void testEnumParameter()
  {
    final Object[] row = scan().get(QueryContextParameters.VECTORIZE.getName());
    Assertions.assertEquals("ENUM", value(row, "type"));
    Assertions.assertEquals("true", value(row, "default_value"));
    Assertions.assertEquals("PUBLIC", value(row, "visibility"));
    Assertions.assertEquals(List.of("JSON", "SQL"), value(row, "query_languages"));
    Assertions.assertEquals(List.of("NATIVE"), value(row, "engines"));
    // Applies to all native query types and statement types.
    Assertions.assertEquals(List.of(), value(row, "native_query_types"));
    Assertions.assertEquals(List.of(), value(row, "statement_types"));
    Assertions.assertEquals("0.16.0-incubating", value(row, "since"));
    Assertions.assertNull(value(row, "deprecation_message"));
  }

  @Test
  public void testNativeQueryTypeAndRange()
  {
    final Object[] row = scan().get(QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING.getName());
    Assertions.assertEquals("INTEGER", value(row, "type"));
    Assertions.assertEquals("[1, 2147483647]", value(row, "range"));
    Assertions.assertNull(value(row, "default_value"));
    Assertions.assertEquals("`druid.query.scan.maxRowsQueuedForOrdering`", value(row, "default_description"));
    Assertions.assertEquals(List.of("JSON"), value(row, "query_languages"));
    Assertions.assertEquals(List.of("scan"), value(row, "native_query_types"));
    // Not provided by SQL queries, so statement types don't apply.
    Assertions.assertNull(value(row, "statement_types"));
  }

  @Test
  public void testEffectiveValue()
  {
    final DefaultQueryConfig runtimeConfig = new DefaultQueryConfig(Map.of("timeout", 30000, "useCache", false));
    // The Broker's own view of its dynamic configuration, as bound for QueryConfigProvider on Brokers
    final BrokerViewOfBrokerConfig brokerConfig = new BrokerViewOfBrokerConfig(null, runtimeConfig);
    brokerConfig.setDynamicConfig(
        BrokerDynamicConfig.builder()
                           .withQueryContext(QueryContext.of(Map.of("useCache", true, "vectorize", "force")))
                           .build()
    );
    final Map<String, Object[]> rows = scan(createTable(brokerConfig, CalciteTests.SUPER_USER_AUTH_RESULT));

    // Broker dynamic configuration over the built-in default
    assertEffective(rows, QueryContextParameters.VECTORIZE, "true", "force", "BROKER_DYNAMIC_CONFIG");
    // Broker dynamic configuration over a runtime property
    assertEffective(rows, QueryContextParameters.USE_CACHE, "true", "true", "BROKER_DYNAMIC_CONFIG");
    // Runtime property, no built-in default
    assertEffective(rows, QueryContextParameters.TIMEOUT, null, "30000", "RUNTIME_PROPERTY");
    // Built-in default only
    assertEffective(rows, QueryContextParameters.PRIORITY, "0", "0", "BUILT_IN");
    // No default anywhere
    assertEffective(rows, QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING, null, null, null);
  }

  @Test
  public void testEffectiveValueIsShownAsTheParameterReadsIt()
  {
    final DefaultQueryConfig runtimeConfig = new DefaultQueryConfig(Map.of("vectorize", "FORCE", "priority", "high"));
    final Map<String, Object[]> rows = scan(createTable(runtimeConfig, CalciteTests.SUPER_USER_AUTH_RESULT));

    // Shown in the same form as default_value
    assertEffective(rows, QueryContextParameters.VECTORIZE, "true", "force", "RUNTIME_PROPERTY");
    // A value the parameter rejects is shown as configured
    assertEffective(rows, QueryContextParameters.PRIORITY, "0", "high", "RUNTIME_PROPERTY");
  }

  @Test
  public void testArrayValuesSortElementByElement()
  {
    final List<SystemQueryContextParametersTable.SortableStringList> arrays = new ArrayList<>(List.of(
        sortable("b"),
        sortable("a", "b"),
        sortable(),
        sortable("a")
    ));
    Collections.sort(arrays);
    Assertions.assertEquals(List.of(List.of(), List.of("a"), List.of("a", "b"), List.of("b")), arrays);
  }

  @Test
  public void testNullDynamicOverrideUnsetsRuntimeProperty()
  {
    final DefaultQueryConfig runtimeConfig = new DefaultQueryConfig(Map.of("priority", 5, "timeout", 30000));
    final BrokerViewOfBrokerConfig brokerConfig = new BrokerViewOfBrokerConfig(null, runtimeConfig);
    final Map<String, Object> dynamicContext = new HashMap<>();
    dynamicContext.put("priority", null);
    dynamicContext.put("timeout", null);
    brokerConfig.setDynamicConfig(
        BrokerDynamicConfig.builder().withQueryContext(QueryContext.of(dynamicContext)).build()
    );
    final Map<String, Object[]> rows = scan(createTable(brokerConfig, CalciteTests.SUPER_USER_AUTH_RESULT));

    // Queries get the built-in default again
    assertEffective(rows, QueryContextParameters.PRIORITY, "0", "0", "BUILT_IN");
    // Queries get no default at all
    assertEffective(rows, QueryContextParameters.TIMEOUT, null, null, null);
  }

  private static SystemQueryContextParametersTable createTable(
      final QueryConfigProvider queryConfigProvider,
      final AuthenticationResult authenticationResult
  )
  {
    return new SystemQueryContextParametersTable(
        new DefaultObjectMapper(),
        queryConfigProvider,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        authenticationResult
    );
  }

  private static void assertEffective(
      final Map<String, Object[]> rows,
      final QueryContextParameter<?> parameter,
      final String defaultValue,
      final String effectiveValue,
      final String effectiveSource
  )
  {
    final Object[] row = rows.get(parameter.getName());
    Assertions.assertEquals(defaultValue, value(row, "default_value"), parameter.getName());
    Assertions.assertEquals(effectiveValue, value(row, "effective_value"), parameter.getName());
    Assertions.assertEquals(effectiveSource, value(row, "effective_source"), parameter.getName());
  }

  private Map<String, Object[]> scan()
  {
    return scan(table);
  }

  private static Map<String, Object[]> scan(final SystemQueryContextParametersTable table)
  {
    return table.scan(null)
                .toList()
                .stream()
                .collect(Collectors.toMap(row -> (String) row[0], Function.identity()));
  }

  private static Object value(final Object[] row, final String column)
  {
    final int index = SystemQueryContextParametersTable.ROW_SIGNATURE.indexOf(column);
    Assertions.assertTrue(index >= 0, column);
    return row[index];
  }

  private static SystemQueryContextParametersTable.SortableStringList sortable(final String... values)
  {
    final SystemQueryContextParametersTable.SortableStringList list =
        new SystemQueryContextParametersTable.SortableStringList();
    list.addAll(List.of(values));
    return list;
  }
}
