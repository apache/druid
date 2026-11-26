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

package org.apache.druid.server;

import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class RunningQueryRegistryTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("evan", "basicAuthorizer", "basic", null);

  private RunningQueryRegistry registry;

  @BeforeEach
  public void setUp()
  {
    registry = new RunningQueryRegistry();
  }

  @Test
  public void testRegisterAndGetAll()
  {
    registry.register(createQuery("query-1", "timeseries", "sql-1", "foo"), AUTHENTICATION_RESULT);

    final List<RunningQueryRegistry.Entry> entries = registry.getAll();
    Assertions.assertEquals(1, entries.size());

    final RunningQueryRegistry.Entry entry = entries.get(0);
    Assertions.assertEquals("query-1", entry.getQueryId());
    Assertions.assertEquals("sql-1", entry.getSqlQueryId());
    Assertions.assertEquals("timeseries", entry.getQueryType());
    Assertions.assertEquals(List.of("foo"), entry.getDatasources());
    Assertions.assertEquals("evan", entry.getIdentity());
    Assertions.assertEquals("basic", entry.getAuthenticator());
    Assertions.assertNotNull(entry.getStartTime());
  }

  @Test
  public void testRegisterWithoutSqlQueryId()
  {
    registry.register(createQuery("query-1", "groupBy", null, "foo"), AUTHENTICATION_RESULT);

    Assertions.assertNull(registry.getAll().get(0).getSqlQueryId());
  }

  @Test
  public void testRegisterWithoutAuthenticationResult()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null);

    final RunningQueryRegistry.Entry entry = registry.getAll().get(0);
    Assertions.assertNull(entry.getIdentity());
    Assertions.assertNull(entry.getAuthenticator());
  }

  @Test
  public void testUnregister()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), AUTHENTICATION_RESULT);
    Assertions.assertEquals(1, registry.getAll().size());

    registry.unregister("query-1");
    Assertions.assertTrue(registry.getAll().isEmpty());

    // Unregistering an unknown query is a no-op.
    registry.unregister("query-1");
    registry.unregister("nonexistent");
    Assertions.assertTrue(registry.getAll().isEmpty());
  }

  @Test
  public void testGetAllIsSortedByStartTime()
  {
    registry.register(createQuery("query-2", "timeseries", null, "bar"), null);
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null);

    final List<RunningQueryRegistry.Entry> entries = registry.getAll();
    Assertions.assertEquals(2, entries.size());
    Assertions.assertTrue(entries.get(0).getStartTime().compareTo(entries.get(1).getStartTime()) <= 0);
  }

  @Test
  public void testRegisterReplacesExistingEntry()
  {
    final Query<?> query = createQuery("query-1", "timeseries", null, "foo");
    registry.register(query, null);
    registry.register(query, null);

    Assertions.assertEquals(1, registry.getAll().size());
  }

  private static Query<?> createQuery(
      final String queryId,
      final String queryType,
      final String sqlQueryId,
      final String datasource
  )
  {
    final Query<?> query = Mockito.mock(Query.class);
    Mockito.when(query.getId()).thenReturn(queryId);
    Mockito.when(query.getType()).thenReturn(queryType);
    Mockito.when(query.getSqlQueryId()).thenReturn(sqlQueryId);
    Mockito.when(query.getDataSource()).thenReturn(new TableDataSource(datasource));
    return query;
  }
}
