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

package org.apache.druid.sql.calcite.run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.RunningQueryRegistry;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.NativeQueryInfo;
import org.apache.druid.sql.http.QueryInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class NativeSqlEngineTest
{
  private static final AuthenticationResult USER =
      new AuthenticationResult("user", "basicAuthorizer", "basic", null);
  private static final AuthenticationResult OTHER_USER =
      new AuthenticationResult("other", "basicAuthorizer", "basic", null);

  private RunningQueryRegistry registry;
  private NativeSqlEngine engine;

  @BeforeEach
  public void setUp()
  {
    registry = new RunningQueryRegistry();
    engine = new NativeSqlEngine(null, null, (SqlStatementFactory) null, registry);
  }

  @Test
  public void testGetQueriesReturnsRunningQueries() throws Exception
  {
    registry.register(createQuery("query-1", "timeseries", "sql-1", "foo"), null, USER);

    final GetQueriesResponse response =
        engine.getQueries(false, false, USER, AuthorizationResult.ALLOW_NO_RESTRICTION);

    Assertions.assertEquals(1, response.getQueries().size());
    final QueryInfo queryInfo = response.getQueries().get(0);
    Assertions.assertTrue(queryInfo instanceof NativeQueryInfo);
    Assertions.assertEquals(NativeSqlEngine.NAME, queryInfo.engine());
    Assertions.assertEquals("RUNNING", queryInfo.state());
    Assertions.assertEquals("query-1", queryInfo.executionId());

    final NativeQueryInfo nativeQueryInfo = (NativeQueryInfo) queryInfo;
    Assertions.assertEquals("query-1", nativeQueryInfo.getQueryId());
    Assertions.assertEquals("sql-1", nativeQueryInfo.getSqlQueryId());
    Assertions.assertEquals("timeseries", nativeQueryInfo.getQueryType());
    Assertions.assertEquals(List.of("foo"), nativeQueryInfo.getDatasources());
    Assertions.assertEquals("user", nativeQueryInfo.getIdentity());
    Assertions.assertEquals("basic", nativeQueryInfo.getAuthenticator());
  }

  @Test
  public void testGetQueriesFiltersByIdentityWithoutStateRead()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, USER);
    registry.register(createQuery("query-2", "timeseries", null, "bar"), null, OTHER_USER);

    final GetQueriesResponse ownQueries = engine.getQueries(false, false, USER, AuthorizationResult.DENY);
    Assertions.assertEquals(1, ownQueries.getQueries().size());
    Assertions.assertEquals("query-1", ownQueries.getQueries().get(0).executionId());

    final GetQueriesResponse allQueries =
        engine.getQueries(false, false, USER, AuthorizationResult.ALLOW_NO_RESTRICTION);
    Assertions.assertEquals(2, allQueries.getQueries().size());
  }

  @Test
  public void testGetQueriesDoesNotMatchIdentityFromDifferentAuthenticator()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, USER);

    // Same identity string, but a different authenticator: the query must not be visible.
    final AuthenticationResult otherAuthenticator =
        new AuthenticationResult("user", "basicAuthorizer", "ldap", null);
    final GetQueriesResponse response = engine.getQueries(false, false, otherAuthenticator, AuthorizationResult.DENY);

    Assertions.assertTrue(response.getQueries().isEmpty());
  }

  @Test
  public void testGetQueriesHidesQueriesWithoutIdentity()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, null);

    final GetQueriesResponse response = engine.getQueries(false, false, USER, AuthorizationResult.DENY);
    Assertions.assertTrue(response.getQueries().isEmpty());
  }

  @Test
  public void testGetQueriesWithoutStateReadHidesIdentity()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, USER);

    final NativeQueryInfo ownQuery = (NativeQueryInfo) engine.getQueries(false, false, USER, AuthorizationResult.DENY)
                                                             .getQueries()
                                                             .get(0);
    Assertions.assertNull(ownQuery.getIdentity());
    Assertions.assertNull(ownQuery.getAuthenticator());

    final NativeQueryInfo allQuery =
        (NativeQueryInfo) engine.getQueries(false, false, USER, AuthorizationResult.ALLOW_NO_RESTRICTION)
                                .getQueries()
                                .get(0);
    Assertions.assertEquals("user", allQuery.getIdentity());
    Assertions.assertEquals("basic", allQuery.getAuthenticator());
  }

  @Test
  public void testGetQueriesWithoutIdentityOrAuthenticatorIsEmpty()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, USER);

    // Requests without an authenticated identity or authenticator see no queries.
    Assertions.assertTrue(
        engine.getQueries(false, false, new AuthenticationResult("user", "basicAuthorizer", null, null),
                          AuthorizationResult.DENY)
              .getQueries()
              .isEmpty()
    );
    Assertions.assertTrue(
        engine.getQueries(false, false, new AuthenticationResult(null, "basicAuthorizer", "basic", null),
                          AuthorizationResult.DENY)
              .getQueries()
              .isEmpty()
    );
  }

  @Test
  public void testGetQueriesSerializesNativeQueryWhenEnabled() throws Exception
  {
    final ObjectMapper jsonMapper = Mockito.mock(ObjectMapper.class);
    Mockito.when(jsonMapper.writeValueAsString(Mockito.any())).thenReturn("{\"queryType\":\"timeseries\"}");

    final RunningQueryRegistry registryWithText = new RunningQueryRegistry(createServerConfig(true));
    final NativeSqlEngine engineWithText =
        new NativeSqlEngine(null, jsonMapper, (SqlStatementFactory) null, registryWithText);

    registryWithText.register(
        createQuery("query-1", "timeseries", null, "foo"),
        "SELECT * FROM foo",
        USER
    );

    final NativeQueryInfo queryInfo = (NativeQueryInfo) engineWithText.getQueries(
        false,
        false,
        USER,
        AuthorizationResult.ALLOW_NO_RESTRICTION
    ).getQueries().get(0);

    Assertions.assertEquals("SELECT * FROM foo", queryInfo.getSql());
    Assertions.assertEquals("{\"queryType\":\"timeseries\"}", queryInfo.getNativeQuery());
  }

  @Test
  public void testGetQueriesOmitsNativeQueryWhenSerializationFails() throws Exception
  {
    final ObjectMapper jsonMapper = Mockito.mock(ObjectMapper.class);
    Mockito.when(jsonMapper.writeValueAsString(Mockito.any()))
           .thenThrow(new JsonProcessingException("fail")
           {
           });

    final RunningQueryRegistry registryWithText = new RunningQueryRegistry(createServerConfig(true));
    final NativeSqlEngine engineWithText =
        new NativeSqlEngine(null, jsonMapper, (SqlStatementFactory) null, registryWithText);

    registryWithText.register(
        createQuery("query-1", "timeseries", null, "foo"),
        "SELECT * FROM foo",
        USER
    );

    final NativeQueryInfo queryInfo = (NativeQueryInfo) engineWithText.getQueries(
        false,
        false,
        USER,
        AuthorizationResult.ALLOW_NO_RESTRICTION
    ).getQueries().get(0);

    Assertions.assertEquals("SELECT * FROM foo", queryInfo.getSql());
    Assertions.assertNull(queryInfo.getNativeQuery());
  }

  @Test
  public void testGetQueriesReturnsEmptyAfterUnregister()
  {
    registry.register(createQuery("query-1", "timeseries", null, "foo"), null, USER);
    registry.unregister("query-1");

    final GetQueriesResponse response =
        engine.getQueries(false, false, USER, AuthorizationResult.ALLOW_NO_RESTRICTION);

    Assertions.assertTrue(response.getQueries().isEmpty());
  }

  @Test
  public void testGetQueriesSerialization() throws Exception
  {
    registry.register(createQuery("query-1", "timeseries", "sql-1", "foo"), null, USER);

    final ObjectMapper mapper = new DefaultObjectMapper();
    new SqlModule().getJacksonModules().forEach(mapper::registerModule);

    final String json = mapper.writeValueAsString(
        engine.getQueries(false, false, USER, AuthorizationResult.ALLOW_NO_RESTRICTION)
    );

    Assertions.assertTrue(json.contains("\"engine\":\"native\""), json);
    Assertions.assertTrue(json.contains("\"queryId\":\"query-1\""), json);
    Assertions.assertTrue(json.contains("\"sqlQueryId\":\"sql-1\""), json);
    Assertions.assertTrue(json.contains("\"state\":\"RUNNING\""), json);
    Assertions.assertTrue(json.contains("\"datasources\":[\"foo\"]"), json);
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

  private static ServerConfig createServerConfig(final boolean enableRunningQueryText) throws Exception
  {
    return new DefaultObjectMapper().readValue(
        "{\"enableRunningQueryText\":" + enableRunningQueryText + "}",
        ServerConfig.class
    );
  }
}
