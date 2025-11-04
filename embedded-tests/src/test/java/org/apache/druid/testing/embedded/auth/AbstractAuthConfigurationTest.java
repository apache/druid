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

package org.apache.druid.testing.embedded.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.avatica.DruidAvaticaJsonHandler;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class AbstractAuthConfigurationTest extends EmbeddedClusterTestBase
{
  protected static final String INVALID_NAME = "invalid%2Fname";

  protected static final String SYS_SCHEMA_SEGMENTS_QUERY =
      "SELECT segment_id, num_rows, size FROM sys.segments WHERE datasource = 'auth_test'";

  protected static final String SYS_SCHEMA_SERVERS_QUERY =
      "SELECT server, host, plaintext_port, tls_port, server_type, tier, curr_size, max_size, is_leader"
      + " FROM sys.servers WHERE tier IS NOT NULL";

  protected static final String SYS_SCHEMA_SERVER_SEGMENTS_QUERY =
      "SELECT * FROM sys.server_segments WHERE segment_id LIKE 'auth_test%'";

  protected static final String SYS_SCHEMA_TASKS_QUERY =
      "SELECT task_id, group_id, type, datasource, status, location"
      + " FROM sys.tasks WHERE datasource IN ('auth_test')";

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
          new Resource(QueryContexts.ENGINE, ResourceType.QUERY_CONTEXT),
          Action.WRITE
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

  protected String adminSegments;
  protected String adminTasks;
  protected String adminServers;
  protected String adminServerSegments;

  protected final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;

  protected HttpClient httpClient;
  protected Map<User, HttpClient> httpClients;

  protected abstract void setupDatasourceOnlyUser();
  protected abstract void setupDatasourceAndContextParamsUser();
  protected abstract void setupDatasourceAndSysTableUser();
  protected abstract void setupDatasourceAndSysAndStateUser();
  protected abstract void setupSysTableAndStateOnlyUser();
  protected abstract void setupTestSpecificHttpClients();
  protected abstract String getAuthenticatorName();
  protected abstract String getAuthorizerName();
  protected abstract String getExpectedAvaticaAuthError();
  protected abstract String getExpectedAvaticaAuthzError();
  protected abstract EmbeddedResource getAuthResource();

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
  
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final EmbeddedBroker broker = new EmbeddedBroker().setServerMemory(500_000_000);
  private final EmbeddedHistorical historical = new EmbeddedHistorical().setServerMemory(500_000_000);
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000)
      .addProperty("druid.worker.capacity", "2");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(getAuthResource())
        .addCommonProperty("druid.auth.unsecuredPaths", "[\"/druid/coordinator/v1/loadqueue\"]")
        .addCommonProperty("druid.auth.authorizeQueryContextParams", "true")
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .addCommonProperty("druid.sql.planner.authorizeSystemTablesDirectly", "true")
        .addCommonProperty("druid.server.http.allowedHttpMethods", "[\"OPTIONS\"]")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(historical)
        .addServer(router);
  }

  @BeforeAll
  public void setupDataAndRoles()
  {
    httpClient = router.bindings().globalHttpClient();

    // Ingest some data
    final String dataSource = "auth_test";
    final String taskId = IdUtils.getRandomId();
    final Task task = MoreResources.Task.BASIC_INDEX
        .get()
        .segmentGranularity("YEAR")
        .dataSource(dataSource)
        .withId(taskId);

    cluster.callApi().runTask(task, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    setupHttpClientsAndUsers();
    setExpectedSystemSchemaObjects(dataSource, taskId);
  }

  @Test
  public void test_systemSchemaAccess_admin()
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    // check that admin access works on all nodes
    checkNodeAccess(adminClient);

    // as admin
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    verifySystemSchemaServerQuery(
        adminClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments
    );

    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceOnlyUser()
  {
    final HttpClient datasourceOnlyUserClient = getHttpClient(User.DATASOURCE_ONLY_USER);
    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceOnlyUserClient,
        HttpMethod.GET,
        getServerUrl(broker) + "/druid/v2/datasources/auth_test"
    );

    // as user that can only read auth_test
    final String expectedMsg = "{\"Access-Check-Result\":\"" + Access.DEFAULT_ERROR_MESSAGE + "\"}";
    verifySystemSchemaQueryIsForbidden(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        expectedMsg
    );

    verifySystemSchemaQueryIsForbidden(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        expectedMsg
    );

    verifySystemSchemaQueryIsForbidden(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        expectedMsg
    );

    verifySystemSchemaQueryIsForbidden(
        datasourceOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        expectedMsg
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceAndSysUser()
  {
    final HttpClient datasourceAndSysUserClient = getHttpClient(User.DATASOURCE_AND_SYS_USER);
    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceAndSysUserClient,
        HttpMethod.GET,
        getServerUrl(broker) + "/druid/v2/datasources/auth_test"
    );

    // as user that can only read auth_test
    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    verifySystemSchemaQueryIsForbidden(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers: Unauthorized\"}"
    );

    verifySystemSchemaQueryIsForbidden(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers: Unauthorized\"}"
    );

    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceAndSysWithStateUser()
  {
    final HttpClient datasourceWithStateUserClient = getHttpClient(User.DATASOURCE_WITH_STATE_USER);
    // check that we can access a state-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceWithStateUserClient,
        HttpMethod.GET,
        getServerUrl(broker) + "/status"
    );

    // as user that can read auth_test and STATE
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    verifySystemSchemaServerQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments
    );

    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_systemSchemaAccess_stateOnlyUser()
  {
    final HttpClient stateOnlyUserClient = getHttpClient(User.STATE_ONLY_USER);
    HttpUtil.makeRequest(stateOnlyUserClient, HttpMethod.GET, getServerUrl(broker) + "/status");

    // as user that can only read STATE
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        "segment_id,num_rows,size"
    );

    verifySystemSchemaServerQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        "server,segment_id"
    );

    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        "task_id,group_id,type,datasource,status,location"
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
  public void test_msqQueryWithContext_datasourceOnlyUser_fail()
  {
    final String query = "select count(*) from auth_test";
    makeMSQQueryRequest(
        getHttpClient(User.DATASOURCE_ONLY_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-denied"),
        HttpResponseStatus.FORBIDDEN
    );
  }

  @Test
  public void test_msqQueryWithContext_datasourceAndContextParamsUser_succeed() throws Exception
  {
    final String query = "select count(*) from auth_test";
    StatusResponseHolder responseHolder = makeMSQQueryRequest(
        getHttpClient(User.DATASOURCE_AND_CONTEXT_PARAMS_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-allowed"),
        HttpResponseStatus.ACCEPTED
    );
    String taskId = jsonMapper.readValue(responseHolder.getContent(), SqlTaskStatus.class).getTaskId();
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  @Test
  public void test_dartQueryWithContext_datasourceOnlyUser_fail()
  {
    final String query = "select count(*) from auth_test";
    makeDartQueryRequest(
        getHttpClient(User.DATASOURCE_ONLY_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-denied"),
        HttpResponseStatus.FORBIDDEN
    );
  }

  @Test
  public void test_dartQueryWithContext_datasourceAndContextParamsUser_succeed()
  {
    final String query = "select count(*) from auth_test";
    makeDartQueryRequest(
        getHttpClient(User.DATASOURCE_AND_CONTEXT_PARAMS_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-allowed"),
        HttpResponseStatus.OK
    );
  }

  @Test
  public void test_sqlQueryWithContext_datasourceOnlyUser_fail()
  {
    final String query = "select count(*) from auth_test";
    makeSQLQueryRequest(
        getHttpClient(User.DATASOURCE_ONLY_USER),
        query,
        ImmutableMap.of("auth_test_ctx", "should-be-denied"),
        HttpResponseStatus.FORBIDDEN
    );
  }

  @Test
  public void test_sqlQueryWithContext_datasourceAndContextParamsUser_succeed()
  {
    final String query = "select count(*) from auth_test";
    makeSQLQueryRequest(
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

  protected void setupHttpClientsAndUsers()
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
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(coordinator) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(overlord) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(broker) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(historical) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(router) + "/status");
  }

  protected void checkLoadStatus(HttpClient httpClient) throws Exception
  {
    checkLoadStatusSingle(httpClient, getServerUrl(coordinator));
    checkLoadStatusSingle(httpClient, getServerUrl(overlord));
    checkLoadStatusSingle(httpClient, getServerUrl(broker));
    checkLoadStatusSingle(httpClient, getServerUrl(historical));
    checkLoadStatusSingle(httpClient, getServerUrl(router));
  }

  protected void testOptionsRequests(HttpClient httpClient)
  {
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(coordinator) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(overlord) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(broker) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(historical) + "/status");
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(router) + "/status");
  }

  protected void checkUnsecuredCoordinatorLoadQueuePath(HttpClient client)
  {
    HttpUtil.makeRequest(client, HttpMethod.GET, getServerUrl(coordinator) + "/druid/coordinator/v1/loadqueue");
  }

  private Properties getAvaticaConnectionPropertiesForAdmin()
  {
    return getAvaticaConnectionPropertiesForUser(User.ADMIN);
  }

  protected void testAvaticaQuery(Properties connectionProperties, String url)
  {
    try (
        Connection connection = DriverManager.getConnection(url, connectionProperties);
        Statement statement = connection.createStatement()) {
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      ResultSet resultSet = statement.executeQuery(query);
      Assertions.assertTrue(resultSet.next());
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
    try (
        Connection connection = DriverManager.getConnection(url, connectionProperties);
        Statement statement = connection.createStatement()) {
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      statement.executeQuery(query);
    }
    catch (AvaticaSqlException ase) {
      Assertions.assertEquals(
          ase.getErrorMessage(),
          expectedError
      );
      return;
    }
    Assertions.fail("Test failed, did not get AvaticaSqlException.");
  }

  protected void checkLoadStatusSingle(HttpClient httpClient, String baseUrl) throws Exception
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authentication/loadStatus"
    );
    String content = holder.getContent();
    Map<String, Boolean> loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    String authenticatorName = getAuthenticatorName();
    Assertions.assertNotNull(loadStatus.get(authenticatorName));
    Assertions.assertTrue(loadStatus.get(authenticatorName));

    holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authorization/loadStatus"
    );
    content = holder.getContent();
    loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    String authorizerName = getAuthorizerName();
    Assertions.assertNotNull(loadStatus.get(authorizerName));
    Assertions.assertTrue(loadStatus.get(authorizerName));
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      HttpResponseStatus expectedStatus
  )
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql", ImmutableMap.of(), expectedStatus);
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  )
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql", context, expectedStatus);
  }

  protected StatusResponseHolder makeMSQQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  )
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql/task", context, expectedStatus);
  }

  protected StatusResponseHolder makeDartQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  )
  {
    final Map<String, Object> dartContext = new HashMap<>(context);
    dartContext.put(QueryContexts.ENGINE, DartSqlEngine.NAME);
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql", dartContext, expectedStatus);
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      String path,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  )
  {
    Map<String, Object> queryMap = ImmutableMap.of(
        "query", query,
        "context", context,
        "resultFormat", "csv",
        "header", true
    );
    return HttpUtil.makeRequest(
        httpClient,
        HttpMethod.POST,
        getServerUrl(broker) + path,
        queryMap,
        expectedStatus
    );
  }

  protected void verifySystemSchemaQueryBase(
      HttpClient client,
      String query,
      String expectedResults
  )
  {
    StatusResponseHolder responseHolder = makeSQLQueryRequest(client, query, HttpResponseStatus.OK);
    String content = responseHolder.getContent().trim();
    Assertions.assertEquals(expectedResults, content);
  }

  protected void verifySystemSchemaQuery(
      HttpClient client,
      String query,
      String expectedResults
  )
  {
    verifySystemSchemaQueryBase(client, query, expectedResults);
  }

  protected void verifySystemSchemaServerQuery(
      HttpClient client,
      String query,
      String expectedResults
  )
  {
    verifySystemSchemaQueryBase(client, query, expectedResults);
  }

  private void verifySystemSchemaQueryIsForbidden(
      HttpClient client,
      String query,
      String expectedErrorMessage
  )
  {
    StatusResponseHolder responseHolder = makeSQLQueryRequest(client, query, HttpResponseStatus.FORBIDDEN);
    Assertions.assertEquals(responseHolder.getStatus(), HttpResponseStatus.FORBIDDEN);
    Assertions.assertEquals(responseHolder.getContent(), expectedErrorMessage);
  }

  protected String getBrokerAvacticaUrl()
  {
    return "jdbc:avatica:remote:url=" + getServerUrl(broker) + DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  protected String getRouterAvacticaUrl()
  {
    return "jdbc:avatica:remote:url=" + getServerUrl(router) + DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  protected void verifyAdminOptionsRequest()
  {
    testOptionsRequests(getHttpClient(User.ADMIN));
  }

  protected void verifyAuthenticationInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authentication/listen/%s",
        getServerUrl(coordinator),
        INVALID_NAME
    ));
  }

  protected void verifyAuthorizationInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authorization/listen/users/%s",
        getServerUrl(coordinator),
        INVALID_NAME
    ));
  }

  protected void verifyGroupMappingsInvalidAuthNameFails()
  {
    verifyInvalidAuthNameFails(StringUtils.format(
        "%s/druid-ext/basic-security/authorization/listen/groupMappings/%s",
        getServerUrl(coordinator),
        INVALID_NAME
    ));
  }

  protected void verifyInvalidAuthNameFails(String endpoint)
  {
    HttpUtil.makeRequest(
        getHttpClient(User.ADMIN),
        HttpMethod.POST,
        endpoint,
        "SERIALIZED_DATA",
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
    StatusResponseHolder responseHolder = HttpUtil.makeRequest(
        maliciousClient,
        HttpMethod.GET,
        getServerUrl(broker) + "/status",
        null,
        HttpResponseStatus.UNAUTHORIZED
    );
    String responseContent = responseHolder.getContent();
    Assertions.assertTrue(responseContent.contains("<tr><th>MESSAGE:</th><td>Unauthorized</td></tr>"));
    Assertions.assertFalse(responseContent.contains(maliciousUsername));
  }

  protected void setupHttpClients()
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

  protected void setExpectedSystemSchemaObjects(String dataSource, String taskId)
  {
    // initial setup is done now, run the system schema response content tests
    final Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    Assertions.assertEquals(1, segments.size());

    final DataSegment segment = Iterables.getOnlyElement(segments);
    final String segmentId = segment.getId().toString();

    adminSegments = StringUtils.format(
        "segment_id,num_rows,size\n"
        + "%s,10,%s",
        segmentId, segment.getSize()
    );

    adminTasks = StringUtils.format(
        "task_id,group_id,type,datasource,status,location\n"
        + "%s,%s,index,%s,SUCCESS,localhost:8091",
        taskId, taskId, dataSource
    );

    adminServers =
        "server,host,plaintext_port,tls_port,server_type,tier,curr_size,max_size,is_leader\n"
        + "localhost:8083,localhost,8083,-1,historical,_default_tier,1939,100000000,\n"
        + "localhost:8091,localhost,8091,-1,indexer,_default_tier,0,0,";

    adminServerSegments = StringUtils.format(
        "server,segment_id\n"
        + "localhost:8083,%s",
        segmentId
    );
  }

  protected String getCoordinatorUrl()
  {
    return getServerUrl(coordinator);
  }

  protected String getBrokerUrl()
  {
    return getServerUrl(broker);
  }
  
  private static String getServerUrl(EmbeddedDruidServer<?> server)
  {
    final DruidNode node = server.bindings().selfNode();
    return StringUtils.format(
        "http://%s:%s",
        node.getHost(),
        node.getPlaintextPort()
    );
  }

  /**
   * curr_size on historicals changes because cluster state is not isolated across
   * different
   * integration tests, zero it out for consistent test results
   * version and start_time are not configurable therefore we zero them as well
   */
  protected static List<Map<String, Object>> getServersWithoutNonConfigurableFields(List<Map<String, Object>> servers)
  {
    return Lists.transform(
        servers,
        (server) -> {
          Map<String, Object> newServer = new HashMap<>(server);
          newServer.put("curr_size", 0);
          newServer.put("start_time", "0");
          newServer.put("version", "0.0.0");
          return newServer;
        }
    );
  }  
}
