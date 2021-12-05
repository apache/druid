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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.avatica.DruidAvaticaJsonHandler;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@ExtensionPoint
public abstract class AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(AbstractAuthConfigurationTest.class);
  protected static final String INVALID_NAME = "invalid%2Fname";

  protected static final String SYSTEM_SCHEMA_SEGMENTS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_segments.json";
  protected static final String SYSTEM_SCHEMA_SERVER_SEGMENTS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_server_segments.json";
  protected static final String SYSTEM_SCHEMA_SERVERS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_servers.json";
  protected static final String SYSTEM_SCHEMA_TASKS_RESULTS_RESOURCE =
      "/results/auth_test_sys_schema_tasks.json";

  protected static final String SYS_SCHEMA_SEGMENTS_QUERY =
      "SELECT * FROM sys.segments WHERE datasource IN ('auth_test')";

  protected static final String SYS_SCHEMA_SERVERS_QUERY =
      "SELECT * FROM sys.servers WHERE tier IS NOT NULL";

  protected static final String SYS_SCHEMA_SERVER_SEGMENTS_QUERY =
      "SELECT * FROM sys.server_segments WHERE segment_id LIKE 'auth_test%'";

  protected static final String SYS_SCHEMA_TASKS_QUERY =
      "SELECT * FROM sys.tasks WHERE datasource IN ('auth_test')";

  protected static final TypeReference<List<Map<String, Object>>> SYS_SCHEMA_RESULTS_TYPE_REFERENCE =
      new TypeReference<List<Map<String, Object>>>()
      {
      };

  /**
   * create a ResourceAction set of permissions that can only read a 'auth_test' datasource, for Authorizer
   * implementations which use ResourceAction pattern matching
   */
  protected static final List<ResourceAction> DATASOURCE_ONLY_PERMISSIONS = Collections.singletonList(
      new ResourceAction(
          new Resource("auth_test", ResourceType.DATASOURCE),
          Action.READ
      )
  );

  protected static final List<ResourceAction> DATASOURCE_QUERY_CONTEXT_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource("auth_test", ResourceType.DATASOURCE),
          Action.READ
      ),
      new ResourceAction(
          new Resource("auth_test_ctx", ResourceType.QUERY_CONTEXT),
          Action.WRITE
      )
  );

  /**
   * create a ResourceAction set of permissions that can only read 'auth_test' + partial SYSTEM_TABLE, for Authorizer
   * implementations which use ResourceAction pattern matching
   */
  protected static final List<ResourceAction> DATASOURCE_SYS_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource("auth_test", ResourceType.DATASOURCE),
          Action.READ
      ),
      new ResourceAction(
          new Resource("segments", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),
      // test missing state permission but having servers permission
      new ResourceAction(
          new Resource("servers", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),
      // test missing state permission but having server_segments permission
      new ResourceAction(
          new Resource("server_segments", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),
      new ResourceAction(
          new Resource("tasks", ResourceType.SYSTEM_TABLE),
          Action.READ
      )
  );

  /**
   * create a ResourceAction set of permissions that can only read 'auth_test' + STATE + SYSTEM_TABLE read access, for
   * Authorizer implementations which use ResourceAction pattern matching
   */
  protected static final List<ResourceAction> DATASOURCE_SYS_STATE_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource("auth_test", ResourceType.DATASOURCE),
          Action.READ
      ),
      new ResourceAction(
          new Resource(".*", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),
      new ResourceAction(
          new Resource(".*", ResourceType.STATE),
          Action.READ
      )
  );

  /**
   * create a ResourceAction set of permissions with only STATE and SYSTEM_TABLE read access, for Authorizer
   * implementations which use ResourceAction pattern matching
   */
  protected static final List<ResourceAction> STATE_ONLY_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource(".*", ResourceType.STATE),
          Action.READ
      ),
      new ResourceAction(
          new Resource(".*", ResourceType.SYSTEM_TABLE),
          Action.READ
      )
  );

  protected enum User
  {
    ADMIN("admin", "priest"),
    DATASOURCE_ONLY_USER("datasourceOnlyUser", "helloworld"),
    DATASOURCE_AND_CONTEXT_PARAMS_USER("datasourceAndContextParamsUser", "helloworld"),
    DATASOURCE_AND_SYS_USER("datasourceAndSysUser", "helloworld"),
    DATASOURCE_WITH_STATE_USER("datasourceWithStateUser", "helloworld"),
    STATE_ONLY_USER("stateOnlyUser", "helloworld"),
    INTERNAL_SYSTEM("druid_system", "warlock");

    private final String name;
    private final String password;

    User(String name, String password)
    {
      this.name = name;
      this.password = password;
    }

    public String getName()
    {
      return name;
    }

    public String getPassword()
    {
      return password;
    }
  }

  protected List<Map<String, Object>> adminSegments;
  protected List<Map<String, Object>> adminTasks;
  protected List<Map<String, Object>> adminServers;
  protected List<Map<String, Object>> adminServerSegments;

  @Inject
  protected IntegrationTestingConfig config;

  @Inject
  protected ObjectMapper jsonMapper;

  @Inject
  @Client
  protected HttpClient httpClient;

  @Inject
  protected CoordinatorResourceTestClient coordinatorClient;

  protected Map<User, HttpClient> httpClients;

  protected abstract void setupDatasourceOnlyUser() throws Exception;
  protected abstract void setupDatasourceAndContextParamsUser() throws Exception;
  protected abstract void setupDatasourceAndSysTableUser() throws Exception;
  protected abstract void setupDatasourceAndSysAndStateUser() throws Exception;
  protected abstract void setupSysTableAndStateOnlyUser() throws Exception;
  protected abstract void setupTestSpecificHttpClients() throws Exception;
  protected abstract String getAuthenticatorName();
  protected abstract String getAuthorizerName();
  protected abstract String getExpectedAvaticaAuthError();
  protected abstract String getExpectedAvaticaAuthzError();

  /**
   * Returns properties for the admin with an invalid password.
   * Implementations can set any properties for authentication as they need.
   */
  protected abstract Properties getAvaticaConnectionPropertiesForInvalidAdmin();
  /**
   * Returns properties for the given user.
   * Implementations can set any properties for authentication as they need.
   *
   * @see User
   */
  protected abstract Properties getAvaticaConnectionPropertiesForUser(User user);

  @Test
  public void test_systemSchemaAccess_admin() throws Exception
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    // check that admin access works on all nodes
    checkNodeAccess(adminClient);

    // as admin
    LOG.info("Checking sys.segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    LOG.info("Checking sys.servers query as admin...");
    verifySystemSchemaServerQuery(
        adminClient,
        SYS_SCHEMA_SERVERS_QUERY,
        getServersWithoutCurrentSize(adminServers)
    );

    LOG.info("Checking sys.server_segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments
    );

    LOG.info("Checking sys.tasks query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceOnlyUser() throws Exception
  {
    final HttpClient datasourceOnlyUserClient = getHttpClient(User.DATASOURCE_ONLY_USER);
    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceOnlyUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/druid/v2/datasources/auth_test",
        null
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.servers query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.server_segments query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.tasks query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Allowed:false, Message:\"}"
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceAndSysUser() throws Exception
  {
    final HttpClient datasourceAndSysUserClient = getHttpClient(User.DATASOURCE_AND_SYS_USER);
    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceAndSysUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/druid/v2/datasources/auth_test",
        null
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceAndSysUser...");
    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments.stream()
                     .filter((segmentEntry) -> "auth_test".equals(segmentEntry.get("datasource")))
                     .collect(Collectors.toList())
    );

    LOG.info("Checking sys.servers query as datasourceAndSysUser...");
    verifySystemSchemaQueryFailure(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.server_segments query as datasourceAndSysUser...");
    verifySystemSchemaQueryFailure(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.tasks query as datasourceAndSysUser...");
    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks.stream()
                  .filter((taskEntry) -> "auth_test".equals(taskEntry.get("datasource")))
                  .collect(Collectors.toList())
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceAndSysWithStateUser() throws Exception
  {
    final HttpClient datasourceWithStateUserClient = getHttpClient(User.DATASOURCE_WITH_STATE_USER);
    // check that we can access a state-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceWithStateUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/status",
        null
    );

    // as user that can read auth_test and STATE
    LOG.info("Checking sys.segments query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments.stream()
                     .filter((segmentEntry) -> "auth_test".equals(segmentEntry.get("datasource")))
                     .collect(Collectors.toList())
    );

    LOG.info("Checking sys.servers query as datasourceWithStateUser...");
    verifySystemSchemaServerQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    LOG.info("Checking sys.server_segments query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments.stream()
                           .filter((serverSegmentEntry) -> ((String) serverSegmentEntry.get("segment_id")).contains(
                               "auth_test"))
                           .collect(Collectors.toList())
    );

    LOG.info("Checking sys.tasks query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks.stream()
                  .filter((taskEntry) -> "auth_test".equals(taskEntry.get("datasource")))
                  .collect(Collectors.toList())
    );
  }

  @Test
  public void test_systemSchemaAccess_stateOnlyUser() throws Exception
  {
    final HttpClient stateOnlyUserClient = getHttpClient(User.STATE_ONLY_USER);
    HttpUtil.makeRequest(stateOnlyUserClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);

    // as user that can only read STATE
    LOG.info("Checking sys.segments query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.servers query as stateOnlyUser...");
    verifySystemSchemaServerQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    LOG.info("Checking sys.server_segments query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.tasks query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        Collections.emptyList()
    );
  }

  @Test
  public void test_unsecuredPathWithoutCredentials_allowed()
  {
    // check that we are allowed to access unsecured path without credentials.
    checkUnsecuredCoordinatorLoadQueuePath(httpClient);
  }

  @Test
  public void test_admin_loadStatus() throws Exception
  {
    checkLoadStatus(getHttpClient(User.ADMIN));
  }

  @Test
  public void test_admin_hasNodeAccess()
  {
    checkNodeAccess(getHttpClient(User.ADMIN));
  }

  @Test
  public void test_internalSystemUser_hasNodeAccess()
  {
    checkNodeAccess(getHttpClient(User.INTERNAL_SYSTEM));
  }

  @Test
  public void test_avaticaQuery_broker()
  {
    final Properties properties = getAvaticaConnectionPropertiesForAdmin();
    testAvaticaQuery(properties, getBrokerAvacticaUrl());
    testAvaticaQuery(properties, StringUtils.maybeRemoveTrailingSlash(getBrokerAvacticaUrl()));
  }

  @Test
  public void test_avaticaQuery_router()
  {
    final Properties properties = getAvaticaConnectionPropertiesForAdmin();
    testAvaticaQuery(properties, getRouterAvacticaUrl());
    testAvaticaQuery(properties, StringUtils.maybeRemoveTrailingSlash(getRouterAvacticaUrl()));
  }

  @Test
  public void test_avaticaQueryAuthFailure_broker() throws Exception
  {
    final Properties properties = getAvaticaConnectionPropertiesForInvalidAdmin();
    testAvaticaAuthFailure(properties, getBrokerAvacticaUrl());
  }

  @Test
  public void test_avaticaQueryAuthFailure_router() throws Exception
  {
    final Properties properties = getAvaticaConnectionPropertiesForInvalidAdmin();
    testAvaticaAuthFailure(properties, getRouterAvacticaUrl());
  }

  @Test
  public void test_avaticaQueryWithContext_datasourceOnlyUser_fail() throws Exception
  {
    final Properties properties = getAvaticaConnectionPropertiesForUser(User.DATASOURCE_ONLY_USER);
    properties.setProperty("auth_test_ctx", "should-be-denied");
    testAvaticaAuthzFailure(properties, getRouterAvacticaUrl());
  }

  @Test
  public void test_avaticaQueryWithContext_datasourceAndContextParamsUser_succeed()
  {
    final Properties properties = getAvaticaConnectionPropertiesForUser(User.DATASOURCE_AND_CONTEXT_PARAMS_USER);
    properties.setProperty("auth_test_ctx", "should-be-allowed");
    testAvaticaQuery(properties, getRouterAvacticaUrl());
  }

  @Test
  public void test_sqlQueryWithContext_datasourceOnlyUser_fail() throws Exception
  {
    final String query = "select count(*) from auth_test";
    StatusResponseHolder responseHolder = makeSQLQueryRequest(
        getHttpClient(User.DATASOURCE_ONLY_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-denied"),
        HttpResponseStatus.FORBIDDEN
    );
  }

  @Test
  public void test_sqlQueryWithContext_datasourceAndContextParamsUser_succeed() throws Exception
  {
    final String query = "select count(*) from auth_test";
    StatusResponseHolder responseHolder = makeSQLQueryRequest(
        getHttpClient(User.DATASOURCE_AND_CONTEXT_PARAMS_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-allowed"),
        HttpResponseStatus.OK
    );
  }

  @Test
  public void test_admin_optionsRequest()
  {
    verifyAdminOptionsRequest();
  }

  @Test
  public void test_authentication_invalidAuthName_fails()
  {
    verifyAuthenticationInvalidAuthNameFails();
  }

  @Test
  public void test_authorization_invalidAuthName_fails()
  {
    verifyAuthorizationInvalidAuthNameFails();
  }

  @Test
  public void test_groupMappings_invalidAuthName_fails()
  {
    verifyGroupMappingsInvalidAuthNameFails();
  }

  @Test
  public void testMaliciousUser()
  {
    verifyMaliciousUser();
  }

  protected HttpClient getHttpClient(User user)
  {
    return Preconditions.checkNotNull(httpClients.get(user), "http client for user[%s]", user.getName());
  }

  protected void setupHttpClientsAndUsers() throws Exception
  {
    setupHttpClients();
    setupDatasourceOnlyUser();
    setupDatasourceAndContextParamsUser();
    setupDatasourceAndSysTableUser();
    setupDatasourceAndSysAndStateUser();
    setupSysTableAndStateOnlyUser();
  }

  protected void checkNodeAccess(HttpClient httpClient)
  {
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getCoordinatorUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getOverlordUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getHistoricalUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, config.getRouterUrl() + "/status", null);
  }

  protected void checkLoadStatus(HttpClient httpClient) throws Exception
  {
    checkLoadStatusSingle(httpClient, config.getCoordinatorUrl());
    checkLoadStatusSingle(httpClient, config.getOverlordUrl());
    checkLoadStatusSingle(httpClient, config.getBrokerUrl());
    checkLoadStatusSingle(httpClient, config.getHistoricalUrl());
    checkLoadStatusSingle(httpClient, config.getRouterUrl());
  }

  protected void testOptionsRequests(HttpClient httpClient)
  {
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getCoordinatorUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getOverlordUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getBrokerUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getHistoricalUrl() + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, config.getRouterUrl() + "/status", null);
  }

  protected void checkUnsecuredCoordinatorLoadQueuePath(HttpClient client)
  {
    HttpUtil.makeRequest(client, HttpMethod.GET, config.getCoordinatorUrl() + "/druid/coordinator/v1/loadqueue", null);
  }

  private Properties getAvaticaConnectionPropertiesForAdmin()
  {
    return getAvaticaConnectionPropertiesForUser(User.ADMIN);
  }

  protected void testAvaticaQuery(Properties connectionProperties, String url)
  {
    LOG.info("URL: " + url);
    try {
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

  protected void testAvaticaAuthFailure(Properties connectionProperties, String url) throws Exception
  {
    testAvaticaAuthFailure(connectionProperties, url, getExpectedAvaticaAuthError());
  }

  protected void testAvaticaAuthzFailure(Properties connectionProperties, String url) throws Exception
  {
    testAvaticaAuthFailure(connectionProperties, url, getExpectedAvaticaAuthzError());
  }

  protected void testAvaticaAuthFailure(Properties connectionProperties, String url, String expectedError)
      throws Exception
  {
    LOG.info("URL: " + url);
    try {
      Connection connection = DriverManager.getConnection(url, connectionProperties);
      Statement statement = connection.createStatement();
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      statement.executeQuery(query);
    }
    catch (AvaticaSqlException ase) {
      Assert.assertEquals(
          ase.getErrorMessage(),
          expectedError
      );
      return;
    }
    Assert.fail("Test failed, did not get AvaticaSqlException.");
  }

  protected void checkLoadStatusSingle(HttpClient httpClient, String baseUrl) throws Exception
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authentication/loadStatus",
        null
    );
    String content = holder.getContent();
    Map<String, Boolean> loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    String authenticatorName = getAuthenticatorName();
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

    String authorizerName = getAuthorizerName();
    Assert.assertNotNull(loadStatus.get(authorizerName));
    Assert.assertTrue(loadStatus.get(authorizerName));
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      HttpResponseStatus expectedStatus
  ) throws Exception
  {
    return makeSQLQueryRequest(httpClient, query, ImmutableMap.of(), expectedStatus);
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  ) throws Exception
  {
    Map<String, Object> queryMap = ImmutableMap.of(
        "query", query,
        "context", context
    );
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.POST,
        config.getBrokerUrl() + "/druid/v2/sql",
        jsonMapper.writeValueAsBytes(queryMap),
        expectedStatus
    );
  }

  protected void verifySystemSchemaQueryBase(
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

  protected void verifySystemSchemaQuery(
      HttpClient client,
      String query,
      List<Map<String, Object>> expectedResults
  ) throws Exception
  {
    verifySystemSchemaQueryBase(client, query, expectedResults, false);
  }

  protected void verifySystemSchemaServerQuery(
      HttpClient client,
      String query,
      List<Map<String, Object>> expectedResults
  ) throws Exception
  {
    verifySystemSchemaQueryBase(client, query, expectedResults, true);
  }

  protected void verifySystemSchemaQueryFailure(
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

  protected String getBrokerAvacticaUrl()
  {
    return "jdbc:avatica:remote:url=" + config.getBrokerUrl() + DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  protected String getRouterAvacticaUrl()
  {
    return "jdbc:avatica:remote:url=" + config.getRouterUrl() + DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  protected void verifyAdminOptionsRequest()
  {
    testOptionsRequests(getHttpClient(User.ADMIN));
  }

  protected void verifyAuthenticationInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authentication/listen/%s",
        config.getCoordinatorUrl(),
        INVALID_NAME
    ));
  }

  protected void verifyAuthorizationInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authorization/listen/users/%s",
        config.getCoordinatorUrl(),
        INVALID_NAME
    ));
  }

  protected void verifyGroupMappingsInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authorization/listen/groupMappings/%s",
        config.getCoordinatorUrl(),
        INVALID_NAME
    ));
  }

  protected void verifyInvalidAuthNameFails(String endpoint)
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    HttpUtil.makeRequestWithExpectedStatus(
        getHttpClient(User.ADMIN),
        HttpMethod.POST,
        endpoint,
        "SERIALIZED_DATA".getBytes(StandardCharsets.UTF_8),
        HttpResponseStatus.INTERNAL_SERVER_ERROR
    );
  }

  protected void verifyMaliciousUser()
  {
    String maliciousUsername = "<script>alert('hello')</script>";
    HttpClient maliciousClient = new CredentialedHttpClient(
        new BasicCredentials(maliciousUsername, "noPass"),
        httpClient
    );
    StatusResponseHolder responseHolder = HttpUtil.makeRequestWithExpectedStatus(
        maliciousClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/status",
        null,
        HttpResponseStatus.UNAUTHORIZED
    );
    String responseContent = responseHolder.getContent();
    Assert.assertTrue(responseContent.contains("<tr><th>MESSAGE:</th><td>Unauthorized</td></tr>"));
    Assert.assertFalse(responseContent.contains(maliciousUsername));
  }

  protected void setupHttpClients() throws Exception
  {
    setupCommonHttpClients();
    setupTestSpecificHttpClients();
  }

  protected void setupCommonHttpClients()
  {
    httpClients = new HashMap<>();
    for (User user : User.values()) {
      httpClients.put(user, setupHttpClientForUser(user.getName(), user.getPassword()));
    }
  }

  /**
   * Creates a HttpClient with the given user credentials.
   * Implementations can override this method to return a different implementation of HttpClient
   * than the basic CredentialedHttpClient.
   */
  protected HttpClient setupHttpClientForUser(String username, String password)
  {
    return new CredentialedHttpClient(
        new BasicCredentials(username, password),
        httpClient
    );
  }

  protected void setExpectedSystemSchemaObjects() throws IOException
  {
    // initial setup is done now, run the system schema response content tests
    adminSegments = jsonMapper.readValue(
        TestQueryHelper.class.getResourceAsStream(SYSTEM_SCHEMA_SEGMENTS_RESULTS_RESOURCE),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );

    adminTasks = jsonMapper.readValue(
        TestQueryHelper.class.getResourceAsStream(SYSTEM_SCHEMA_TASKS_RESULTS_RESOURCE),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );

    adminServers = getServersWithoutCurrentSize(
        jsonMapper.readValue(
            fillServersTemplate(
                config,
                AbstractIndexerTest.getResourceAsString(SYSTEM_SCHEMA_SERVERS_RESULTS_RESOURCE)
            ),
            SYS_SCHEMA_RESULTS_TYPE_REFERENCE
        )
    );

    adminServerSegments = jsonMapper.readValue(
        fillSegementServersTemplate(
            config,
            AbstractIndexerTest.getResourceAsString(SYSTEM_SCHEMA_SERVER_SEGMENTS_RESULTS_RESOURCE)
        ),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );
  }

  /**
   * curr_size on historicals changes because cluster state is not isolated across different
   * integration tests, zero it out for consistent test results
   */
  protected static List<Map<String, Object>> getServersWithoutCurrentSize(List<Map<String, Object>> servers)
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

  protected static String fillSegementServersTemplate(IntegrationTestingConfig config, String template)
  {
    return StringUtils.replace(template, "%%HISTORICAL%%", config.getHistoricalInternalHost());
  }

  protected static String fillServersTemplate(IntegrationTestingConfig config, String template)
  {
    String json = StringUtils.replace(template, "%%HISTORICAL%%", config.getHistoricalInternalHost());
    json = StringUtils.replace(json, "%%BROKER%%", config.getBrokerInternalHost());
    json = StringUtils.replace(json, "%%NON_LEADER%%", String.valueOf(NullHandling.defaultLongValue()));
    return json;
  }
}
