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

package org.apache.druid.tests.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.HttpUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(AbstractAuthConfigurationTest.class);

  static final TypeReference<List<Map<String, Object>>> SYS_SCHEMA_RESULTS_TYPE_REFERENCE =
      new TypeReference<List<Map<String, Object>>>()
      {
      };

  static final String SYSTEM_SCHEMA_SEGMENTS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_segments.json";
  static final String SYSTEM_SCHEMA_SERVER_SEGMENTS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_server_segments.json";
  static final String SYSTEM_SCHEMA_SERVERS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_servers.json";
  static final String SYSTEM_SCHEMA_TASKS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_tasks.json";

  static final String SYS_SCHEMA_SEGMENTS_QUERY =
      "SELECT * FROM sys.segments WHERE datasource IN ('auth_test')";

  static final String SYS_SCHEMA_SERVERS_QUERY =
      "SELECT * FROM sys.servers WHERE tier IS NOT NULL";

  static final String SYS_SCHEMA_SERVER_SEGMENTS_QUERY =
      "SELECT * FROM sys.server_segments WHERE segment_id LIKE 'auth_test%'";

  static final String SYS_SCHEMA_TASKS_QUERY =
      "SELECT * FROM sys.tasks WHERE datasource IN ('auth_test')";

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @Client
  HttpClient httpClient;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;


  void checkNodeAccess(HttpClient httpClient)
  {
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getCoordinatorUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getOverlordUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getHistoricalUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getRouterUrl() + "/status", null);
  }

  void checkLoadStatus(HttpClient httpClient, String authenticatorName, String authorizerName) throws Exception
  {
    checkLoadStatusSingle(httpClient, config.getCoordinatorUrl(), authenticatorName, authorizerName);
    checkLoadStatusSingle(httpClient, config.getOverlordUrl(), authenticatorName, authorizerName);
    checkLoadStatusSingle(httpClient, config.getBrokerUrl(), authenticatorName, authorizerName);
    checkLoadStatusSingle(httpClient, config.getHistoricalUrl(), authenticatorName, authorizerName);
    checkLoadStatusSingle(httpClient, config.getRouterUrl(), authenticatorName, authorizerName);
  }

  void testOptionsRequests(HttpClient httpClient)
  {
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getCoordinatorUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getOverlordUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getBrokerUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getHistoricalUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getRouterUrl() + "/status", null);
  }

  void checkUnsecuredCoordinatorLoadQueuePath(HttpClient client)
  {
    HttpUtil.makeRequest(client, HttpMethod.GET, config.getCoordinatorUrl() + "/druid/coordinator/v1/loadqueue", null);
  }

  void testAvaticaQuery(String url)
  {
    LOG.info("URL: " + url);
    try {
      Properties connectionProperties = new Properties();
      connectionProperties.setProperty("user", "admin");
      connectionProperties.setProperty("password", "priest");
      Connection connection = DriverManager.getConnection(url, connectionProperties);
      Statement statement = connection.createStatement();
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      ResultSet resultSet = statement.executeQuery(query);
      Assert.assertTrue(resultSet.next());
      statement.close();
      connection.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void testAvaticaAuthFailure(String url, String expectedAuthError) throws Exception
  {
    LOG.info("URL: " + url);
    try {
      Properties connectionProperties = new Properties();
      connectionProperties.setProperty("user", "admin");
      connectionProperties.setProperty("password", "wrongpassword");
      Connection connection = DriverManager.getConnection(url, connectionProperties);
      Statement statement = connection.createStatement();
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      statement.executeQuery(query);
    }
    catch (AvaticaSqlException ase) {
      Assert.assertEquals(
          ase.getErrorMessage(),
          expectedAuthError
      );
      return;
    }
    Assert.fail("Test failed, did not get AvaticaSqlException.");
  }

  private void checkLoadStatusSingle(
      HttpClient httpClient,
      String baseUrl,
      String authenticatorName,
      String authorizerName) throws Exception
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authentication/loadStatus",
        null
    );
    String content = holder.getContent();
    Map<String, Boolean> loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    Assert.assertNotNull(loadStatus.get(authenticatorName));
    Assert.assertTrue(loadStatus.get(authenticatorName));

    holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authorization/loadStatus",
        null
    );
    content = holder.getContent();
    loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    Assert.assertNotNull(loadStatus.get(authorizerName));
    Assert.assertTrue(loadStatus.get(authorizerName));
  }

  StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      HttpResponseStatus expectedStatus
  ) throws Exception
  {
    Map<String, Object> queryMap = ImmutableMap.of(
        "query", query
    );
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.POST,
        config.getBrokerUrl() + "/druid/v2/sql",
        jsonMapper.writeValueAsBytes(queryMap),
        expectedStatus
    );
  }

  void verifySystemSchemaQueryBase(
      HttpClient client,
      String query,
      List<Map<String, Object>> expectedResults,
      boolean isServerQuery
  ) throws Exception
  {
    StatusResponseHolder responseHolder = makeSQLQueryRequest(client, query, HttpResponseStatus.OK);
    String content = responseHolder.getContent();
    List<Map<String, Object>> responseMap = jsonMapper.readValue(content, SYS_SCHEMA_RESULTS_TYPE_REFERENCE);
    if (isServerQuery) {
      responseMap = getServersWithoutCurrentSize(responseMap);
    }
    Assert.assertEquals(responseMap, expectedResults);
  }

  void verifySystemSchemaQuery(
      HttpClient client,
      String query,
      List<Map<String, Object>> expectedResults
  ) throws Exception
  {
    verifySystemSchemaQueryBase(client, query, expectedResults, false);
  }

  void verifySystemSchemaServerQuery(
      HttpClient client,
      String query,
      List<Map<String, Object>> expectedResults
  ) throws Exception
  {
    verifySystemSchemaQueryBase(client, query, expectedResults, true);
  }

  void verifySystemSchemaQueryFailure(
      HttpClient client,
      String query,
      HttpResponseStatus expectedErrorStatus,
      String expectedErrorMessage
  ) throws Exception
  {
    StatusResponseHolder responseHolder = makeSQLQueryRequest(client, query, expectedErrorStatus);
    Assert.assertEquals(responseHolder.getStatus(), expectedErrorStatus);
    Assert.assertEquals(responseHolder.getContent(), expectedErrorMessage);
  }

  /**
   * curr_size on historicals changes because cluster state is not isolated across different
   * integration tests, zero it out for consistent test results
   */
  static List<Map<String, Object>> getServersWithoutCurrentSize(List<Map<String, Object>> servers)
  {
    return Lists.transform(
        servers,
        (server) -> {
          Map<String, Object> newServer = new HashMap<>(server);
          newServer.put("curr_size", 0);
          return newServer;
        }
    );
  }

  static String fillSegementServersTemplate(IntegrationTestingConfig config, String template)
  {
    String json = StringUtils.replace(template, "%%HISTORICAL%%", config.getHistoricalInternalHost());
    return json;
  }

  static String fillServersTemplate(IntegrationTestingConfig config, String template)
  {
    String json = StringUtils.replace(template, "%%HISTORICAL%%", config.getHistoricalInternalHost());
    json = StringUtils.replace(json, "%%BROKER%%", config.getBrokerInternalHost());
    json = StringUtils.replace(json, "%%NON_LEADER%%", String.valueOf(NullHandling.defaultLongValue()));
    return json;
  }
}
