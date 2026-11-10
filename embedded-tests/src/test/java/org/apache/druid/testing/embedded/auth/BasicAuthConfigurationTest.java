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

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class BasicAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final String BASIC_AUTHENTICATOR = "basic";
  private static final String BASIC_AUTHORIZER = "basic";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  // This error must match both authorization paths: initial prepare of
  // the query, and checks of resources used by a query during execution.
  // The two errors are raised in different points in the code, but should
  // look identical to users (and tests).
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  private HttpClient druid99;
  private HttpClient sysTableUser;
  private HttpClient sysTableWithStateUser;
  private HttpClient stateWithoutSysTableUser;

  @Override
  protected EmbeddedResource getAuthResource()
  {
    return new EmbeddedBasicAuthResource();
  }

  @Test
  public void test_druid99User_hasNodeAccess()
  {
    checkNodeAccess(druid99);
  }

  /**
   * With {@code druid.auth.authorizeSystemTablesDirectly}, a native {@code sys.server_properties} query needs both
   * SYSTEM_TABLE READ on the table and STATE READ, which the table's row authorizer requires, on the Broker, the
   * Router, the Coordinator and the Overlord.
   */
  @Test
  public void test_nativeSystemTableQuery_requiresSystemTableAndStateRead()
  {
    assertNativeSystemTableQueryStatus(sysTableWithStateUser, HttpResponseStatus.OK);
    assertNativeSystemTableQueryStatus(sysTableUser, HttpResponseStatus.FORBIDDEN);
    assertNativeSystemTableQueryStatus(stateWithoutSysTableUser, HttpResponseStatus.FORBIDDEN);
  }

  /**
   * With {@code druid.auth.authorizeSystemTablesDirectly}, a SQL query on {@code sys.server_properties} that uses the
   * Druid engine reads the rows of every service: the Broker's node queries, sent with the escalated identity, are
   * authorized on each of them. The caller still needs STATE READ, and SYSTEM_TABLE READ on the table, without which
   * the table isn't visible to the caller at all.
   */
  @Test
  public void test_sqlOnSystemTableWithDruidEngine_gathersFromEveryServiceAndRequiresSystemTableAndStateRead()
  {
    final Map<String, Object> context =
        Map.of(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES.getName(), true);
    final String sql = "SELECT DISTINCT service_name FROM sys.server_properties ORDER BY service_name";

    final String plan = makeSQLQueryRequest(
        getHttpClient(User.ADMIN),
        "EXPLAIN PLAN FOR " + sql,
        context,
        HttpResponseStatus.OK
    ).getContent();
    Assertions.assertTrue(plan.contains("systemTable"), plan);

    final String services = makeSQLQueryRequest(sysTableWithStateUser, sql, context, HttpResponseStatus.OK)
        .getContent();
    for (final String service : List.of("broker", "coordinator", "historical", "indexer", "overlord", "router")) {
      Assertions.assertTrue(services.contains("druid/" + service), services);
    }

    makeSQLQueryRequest(sysTableUser, sql, context, HttpResponseStatus.FORBIDDEN);
    final String invisible =
        makeSQLQueryRequest(stateWithoutSysTableUser, sql, context, HttpResponseStatus.BAD_REQUEST).getContent();
    Assertions.assertTrue(invisible.contains("Object 'server_properties' not found within 'sys'"), invisible);
  }

  @Override
  protected void setupDatasourceOnlyUser()
  {
    createUserAndRoleWithPermissions(
        "datasourceOnlyUser",
        "helloworld",
        "datasourceOnlyRole",
        DATASOURCE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndContextParamsUser()
  {
    createUserAndRoleWithPermissions(
        "datasourceAndContextParamsUser",
        "helloworld",
        "datasourceAndContextParamsRole",
        DATASOURCE_QUERY_CONTEXT_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysTableUser()
  {
    createUserAndRoleWithPermissions(
        "datasourceAndSysUser",
        "helloworld",
        "datasourceAndSysRole",
        DATASOURCE_SYS_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser()
  {
    createUserAndRoleWithPermissions(
        "datasourceWithStateUser",
        "helloworld",
        "datasourceWithStateRole",
        DATASOURCE_SYS_STATE_PERMISSIONS
    );
  }

  @Override
  protected void setupSysTableAndStateOnlyUser()
  {
    createUserAndRoleWithPermissions(
        "stateOnlyUser",
        "helloworld",
        "stateOnlyRole",
        STATE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupTestSpecificHttpClients()
  {
    // create a new user+role that can read /status
    createUserAndRoleWithPermissions(
        "druid",
        "helloworld",
        "druidrole",
        STATE_ONLY_PERMISSIONS
    );

    // create 100 users
    for (int i = 0; i < 100; i++) {
      final String username = "druid" + i;
      postAsAdmin(null, "/authentication/db/basic/users/%s", username);
      postAsAdmin(null, "/authorization/db/basic/users/%s", username);
    }

    // setup the last of 100 users and check that it works
    postAsAdmin(
        new BasicAuthenticatorCredentialUpdate("helloworld", 5000),
        "/authentication/db/basic/users/druid99/credentials"
    );
    postAsAdmin(null, "/authorization/db/basic/users/druid99/roles/druidrole");

    druid99 = new CredentialedHttpClient(
        new BasicCredentials("druid99", "helloworld"),
        httpClient
    );

    final ResourceAction readServerPropertiesTable =
        new ResourceAction(new Resource("server_properties", ResourceType.SYSTEM_TABLE), Action.READ);
    final ResourceAction readState = new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ);
    // The Router sets queryId on native queries it forwards, and this cluster authorizes query context keys.
    final ResourceAction writeQueryId = new ResourceAction(
        new Resource(QueryContextParameters.QUERY_ID.getName(), ResourceType.QUERY_CONTEXT),
        Action.WRITE
    );
    // SQL queries on system tables set useDruidEngineForSystemTables to use the Druid engine.
    final ResourceAction writeUseDruidEngine = new ResourceAction(
        new Resource(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES.getName(), ResourceType.QUERY_CONTEXT),
        Action.WRITE
    );
    sysTableUser = createUserWithClient(
        "sysTableUser",
        List.of(readServerPropertiesTable, writeQueryId, writeUseDruidEngine)
    );
    sysTableWithStateUser = createUserWithClient(
        "sysTableWithStateUser",
        List.of(readServerPropertiesTable, readState, writeQueryId, writeUseDruidEngine)
    );
    stateWithoutSysTableUser = createUserWithClient(
        "stateWithoutSysTableUser",
        List.of(readState, writeQueryId, writeUseDruidEngine)
    );
  }

  @Override
  protected String getAuthenticatorName()
  {
    return BASIC_AUTHENTICATOR;
  }

  @Override
  protected String getAuthorizerName()
  {
    return BASIC_AUTHORIZER;
  }

  @Override
  protected String getExpectedAvaticaAuthError()
  {
    return EXPECTED_AVATICA_AUTH_ERROR;
  }

  @Override
  protected String getExpectedAvaticaAuthzError()
  {
    return EXPECTED_AVATICA_AUTHZ_ERROR;
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForInvalidAdmin()
  {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty("user", "admin");
    connectionProperties.setProperty("password", "invalid_password");
    return connectionProperties;
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForUser(User user)
  {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty("user", user.getName());
    connectionProperties.setProperty("password", user.getPassword());
    return connectionProperties;
  }

  private HttpClient createUserWithClient(final String user, final List<ResourceAction> permissions)
  {
    createUserAndRoleWithPermissions(user, "helloworld", user + "Role", permissions);
    return new CredentialedHttpClient(new BasicCredentials(user, "helloworld"), httpClient);
  }

  private void createUserAndRoleWithPermissions(
      String user,
      String password,
      String role,
      List<ResourceAction> permissions
  )
  {
    EmbeddedBasicAuthResource.createUserWithPermissions(
        getHttpClient(User.ADMIN),
        getCoordinator(),
        user,
        password,
        role,
        permissions
    );
  }

  private void postAsAdmin(
      Object payload,
      String pathFormat,
      Object... pathParams
  )
  {
    HttpClient adminClient = getHttpClient(User.ADMIN);
    String url = getBaseUrl() + StringUtils.format(pathFormat, pathParams);
    HttpUtil.makeRequest(adminClient, HttpMethod.POST, url, payload, HttpResponseStatus.OK);
  }

  private String getBaseUrl()
  {
    return getCoordinatorUrl() + "/druid-ext/basic-security";
  }
}
