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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.ResourceAction;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class BasicAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(BasicAuthConfigurationTest.class);

  private static final String BASIC_AUTHENTICATOR = "basic";
  private static final String BASIC_AUTHORIZER = "basic";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  // This error must match both authorization paths: initial prepare of
  // the query, and checks of resources used by a query during execution.
  // The two errors are raised in different points in the code, but should
  // look identical to users (and tests).
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  private HttpClient druid99;

  @Test
  public void test_druid99User_hasNodeAccess()
  {
    checkNodeAccess(druid99);
  }

  @Override
  protected void setupDatasourceOnlyUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        "datasourceOnlyUser",
        "helloworld",
        "datasourceOnlyRole",
        DATASOURCE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndContextParamsUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        "datasourceAndContextParamsUser",
        "helloworld",
        "datasourceAndContextParamsRole",
        DATASOURCE_QUERY_CONTEXT_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysTableUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        "datasourceAndSysUser",
        "helloworld",
        "datasourceAndSysRole",
        DATASOURCE_SYS_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        "datasourceWithStateUser",
        "helloworld",
        "datasourceWithStateRole",
        DATASOURCE_SYS_STATE_PERMISSIONS
    );
  }

  @Override
  protected void setupSysTableAndStateOnlyUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        "stateOnlyUser",
        "helloworld",
        "stateOnlyRole",
        STATE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupTestSpecificHttpClients() throws Exception
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
      LOG.info("Created user[%s]", username);
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

  private void createUserAndRoleWithPermissions(
      String user,
      String password,
      String role,
      List<ResourceAction> permissions
  ) throws Exception
  {
    // Setup authentication by creating user and password
    postAsAdmin(null, "/authentication/db/basic/users/%s", user);

    final BasicAuthenticatorCredentialUpdate credentials
        = new BasicAuthenticatorCredentialUpdate(password, 5000);
    postAsAdmin(credentials, "/authentication/db/basic/users/%s/credentials", user);

    // Setup authorization by assigning a role to the user
    postAsAdmin(null, "/authorization/db/basic/users/%s", user);
    postAsAdmin(null, "/authorization/db/basic/roles/%s", role);
    postAsAdmin(null, "/authorization/db/basic/users/%s/roles/%s", user, role);
    postAsAdmin(permissions, "/authorization/db/basic/roles/%s/permissions", role);
  }

  private void postAsAdmin(
      Object payload,
      String pathFormat,
      Object... pathParams
  ) throws IOException
  {
    HttpClient adminClient = getHttpClient(User.ADMIN);

    byte[] payloadBytes = payload == null ? null : jsonMapper.writeValueAsBytes(payload);
    String url = getBaseUrl() + StringUtils.format(pathFormat, pathParams);
    HttpUtil.makeRequest(adminClient, HttpMethod.POST, url, payloadBytes);
  }

  private String getBaseUrl()
  {
    return getCoordinatorUrl() + "/druid-ext/basic-security";
  }
}
