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
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
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
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
import java.util.Set;

@ExtensionPoint
public abstract class AbstractAuthConfigurationTest extends EmbeddedClusterTestBase
{
  private static final Logger LOG = new Logger(AbstractAuthConfigurationTest.class);
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
  
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(new EmbeddedBasicAuthResource())
        .addCommonProperty("druid.auth.unsecuredPaths", "[\"/druid/coordinator/v1/loadqueue\"]")
        .addCommonProperty("druid.auth.authorizeQueryContextParams", "true")
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .addCommonProperty("druid.sql.planner.authorizeSystemTablesDirectly", "true")
        .addCommonProperty("druid.server.http.allowedHttpMethods", "[\"OPTIONS\"]")
        .addExtensions(
            MSQSqlModule.class,
            MSQIndexingModule.class,
            SqlTaskModule.class,
            MSQDurableStorageModule.class,
            MSQExternalDataSourceModule.class,
            IndexerMemoryManagementModule.class,
            DartControllerModule.class,
            DartWorkerModule.class,
            DartControllerMemoryManagementModule.class,
            DartWorkerMemoryManagementModule.class
        )
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(new EmbeddedIndexer().setServerMemory(300_000_000).addProperty("druid.worker.capacity", "2"))
        .addServer(broker.setServerMemory(500_000_000))
        .addServer(historical.setServerMemory(500_000_000))
        .addServer(router);
  }

  @BeforeAll
  public void setupDataAndRoles() throws Exception
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
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator);

    setupHttpClientsAndUsers();
    setExpectedSystemSchemaObjects(dataSource, taskId);
  }

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
        adminServers
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
        getServerUrl(broker) + "/druid/v2/datasources/auth_test",
        null
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceOnlyUser...");
    final String expectedMsg = "{\"Access-Check-Result\":\"" + Access.DEFAULT_ERROR_MESSAGE + "\"}";
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        expectedMsg
    );

    LOG.info("Checking sys.servers query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        expectedMsg
    );

    LOG.info("Checking sys.server_segments query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        expectedMsg
    );

    LOG.info("Checking sys.tasks query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        expectedMsg
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
        getServerUrl(broker) + "/druid/v2/datasources/auth_test",
        null
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceAndSysUser...");
    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    LOG.info("Checking sys.servers query as datasourceAndSysUser...");
    verifySystemSchemaQueryFailure(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers: Unauthorized\"}"
    );

    LOG.info("Checking sys.server_segments query as datasourceAndSysUser...");
    verifySystemSchemaQueryFailure(
        datasourceAndSysUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers: Unauthorized\"}"
    );

    LOG.info("Checking sys.tasks query as datasourceAndSysUser...");
    verifySystemSchemaQuery(
        datasourceAndSysUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
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
        getServerUrl(broker) + "/status",
        null
    );

    // as user that can read auth_test and STATE
    LOG.info("Checking sys.segments query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
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
        adminServerSegments
    );

    LOG.info("Checking sys.tasks query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_systemSchemaAccess_stateOnlyUser() throws Exception
  {
    final HttpClient stateOnlyUserClient = getHttpClient(User.STATE_ONLY_USER);
    HttpUtil.makeRequest(stateOnlyUserClient, HttpMethod.GET, getServerUrl(broker) + "/status", null);

    // as user that can only read STATE
    LOG.info("Checking sys.segments query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        "segment_id,num_rows,size"
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
        "server,segment_id"
    );

    LOG.info("Checking sys.tasks query as stateOnlyUser...");
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
  public void test_msqQueryWithContext_datasourceOnlyUser_fail() throws Exception
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
  public void test_dartQueryWithContext_datasourceOnlyUser_fail() throws Exception
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
  public void test_dartQueryWithContext_datasourceAndContextParamsUser_succeed() throws Exception
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
  public void test_sqlQueryWithContext_datasourceOnlyUser_fail() throws Exception
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
  public void test_sqlQueryWithContext_datasourceAndContextParamsUser_succeed() throws Exception
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
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(coordinator) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(overlord) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(broker) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(historical) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.GET, getServerUrl(router) + "/status", null);
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
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(coordinator) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(overlord) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(broker) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(historical) + "/status", null);
    HttpUtil.makeRequest(httpClient, HttpMethod.OPTIONS, getServerUrl(router) + "/status", null);
  }

  protected void checkUnsecuredCoordinatorLoadQueuePath(HttpClient client)
  {
    HttpUtil.makeRequest(client, HttpMethod.GET, getServerUrl(coordinator) + "/druid/coordinator/v1/loadqueue", null);
  }

  private Properties getAvaticaConnectionPropertiesForAdmin()
  {
    return getAvaticaConnectionPropertiesForUser(User.ADMIN);
  }

  protected void testAvaticaQuery(Properties connectionProperties, String url)
  {
    LOG.info("URL: " + url);
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
    LOG.info("URL: " + url);
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
        baseUrl + "/druid-ext/basic-security/authentication/loadStatus",
        null
    );
    String content = holder.getContent();
    Map<String, Boolean> loadStatus = jsonMapper.readValue(content, JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);

    String authenticatorName = getAuthenticatorName();
    Assertions.assertNotNull(loadStatus.get(authenticatorName));
    Assertions.assertTrue(loadStatus.get(authenticatorName));

    holder = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authorization/loadStatus",
        null
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
  ) throws Exception
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql", ImmutableMap.of(), expectedStatus);
  }

  protected StatusResponseHolder makeSQLQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  ) throws Exception
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql", context, expectedStatus);
  }

  protected StatusResponseHolder makeMSQQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  ) throws Exception
  {
    return makeSQLQueryRequest(httpClient, query, "/druid/v2/sql/task", context, expectedStatus);
  }

  protected StatusResponseHolder makeDartQueryRequest(
      HttpClient httpClient,
      String query,
      Map<String, Object> context,
      HttpResponseStatus expectedStatus
  ) throws Exception
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
  ) throws Exception
  {
    Map<String, Object> queryMap = ImmutableMap.of(
        "query", query,
        "context", context,
        "resultFormat", "csv",
        "header", true
    );
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.POST,
        getServerUrl(broker) + path,
        jsonMapper.writeValueAsBytes(queryMap),
        expectedStatus
    );
  }

  protected void verifySystemSchemaQueryBase(
      HttpClient client,
      String query,
      String expectedResults,
      boolean isServerQuery
  ) throws Exception
  {
    StatusResponseHolder responseHolder = makeSQLQueryRequest(client, query, HttpResponseStatus.OK);
    String content = responseHolder.getContent().trim();
    Assertions.assertEquals(expectedResults, content);
  }

  protected void verifySystemSchemaQuery(
      HttpClient client,
      String query,
      String expectedResults
  ) throws Exception
  {
    verifySystemSchemaQueryBase(client, query, expectedResults, false);
  }

  protected void verifySystemSchemaServerQuery(
      HttpClient client,
      String query,
      String expectedResults
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
    Assertions.assertEquals(responseHolder.getStatus(), expectedErrorStatus);
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
        getServerUrl(broker) + "/status",
        null,
        HttpResponseStatus.UNAUTHORIZED
    );
    String responseContent = responseHolder.getContent();
    Assertions.assertTrue(responseContent.contains("<tr><th>MESSAGE:</th><td>Unauthorized</td></tr>"));
    Assertions.assertFalse(responseContent.contains(maliciousUsername));
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
}
