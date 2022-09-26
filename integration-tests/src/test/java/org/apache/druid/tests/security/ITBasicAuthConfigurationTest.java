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

import io.netty.handler.codec.http.HttpMethod;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Properties;

@Test(groups = TestNGGroup.SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthConfigurationTest.class);

  private static final String BASIC_AUTHENTICATOR = "basic";
  private static final String BASIC_AUTHORIZER = "basic";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: QueryInterruptedException: User metadata store authentication failed. -> BasicSecurityAuthenticationException: User metadata store authentication failed.";
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: RuntimeException: org.apache.druid.server.security.ForbiddenException: Allowed:false, Message: -> ForbiddenException: Allowed:false, Message:";

  private HttpClient druid99;

  @BeforeClass
  public void before() throws Exception
  {
    // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
    );

    setupHttpClientsAndUsers();
    setExpectedSystemSchemaObjects();
  }

  @Test
  public void test_druid99User_hasNodeAccess()
  {
    checkNodeAccess(druid99);
  }

  @Override
  protected void setupDatasourceOnlyUser() throws Exception
  {
    createUserAndRoleWithPermissions(
        getHttpClient(User.ADMIN),
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
        getHttpClient(User.ADMIN),
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
        getHttpClient(User.ADMIN),
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
        getHttpClient(User.ADMIN),
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
        getHttpClient(User.ADMIN),
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
        getHttpClient(User.ADMIN),
        "druid",
        "helloworld",
        "druidrole",
        STATE_ONLY_PERMISSIONS
    );

    // create 100 users
    for (int i = 0; i < 100; i++) {
      HttpUtil.makeRequest(
          getHttpClient(User.ADMIN),
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid" + i,
          null
      );

      HttpUtil.makeRequest(
          getHttpClient(User.ADMIN),
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid" + i,
          null
      );

      LOG.info("Finished creating user druid" + i);
    }

    // setup the last of 100 users and check that it works
    HttpUtil.makeRequest(
        getHttpClient(User.ADMIN),
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid99/credentials",
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate("helloworld", 5000))
    );

    HttpUtil.makeRequest(
        getHttpClient(User.ADMIN),
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid99/roles/druidrole",
        null
    );

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
      HttpClient adminClient,
      String user,
      String password,
      String role,
      List<ResourceAction> permissions
  ) throws Exception
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s",
            config.getCoordinatorUrl(),
            user
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s/credentials",
            config.getCoordinatorUrl(),
            user
        ),
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate(password, 5000))
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s",
            config.getCoordinatorUrl(),
            user
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s/roles/%s",
            config.getCoordinatorUrl(),
            user,
            role
        ),
        null
    );
    byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s/permissions",
            config.getCoordinatorUrl(),
            role
        ),
        permissionsBytes
    );
  }
}
