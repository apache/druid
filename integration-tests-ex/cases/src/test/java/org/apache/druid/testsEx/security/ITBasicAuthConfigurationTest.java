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

package org.apache.druid.testsEx.security;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.server.security.Access;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testsEx.categories.BasicAuth;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

/**
 * Tests basic authentication.
 *
 * Note for improvement: this test pulls down a 446MB segment just so we have an entry in
 * the segments table. Docker I/O is quite slow, making this test very slow to run.
 * A better solution is to use MSQ to create a small datasource (we don't actually care
 * about the data), and adjust the tests accordingly.
 */
@RunWith(DruidTestRunner.class)
@Category(BasicAuth.class)
public class ITBasicAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthConfigurationTest.class);

  private static final String BASIC_AUTHENTICATOR = "basic";
  private static final String BASIC_AUTHORIZER = "basic";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  // This error must match both authorization paths: initial prepare of
  // the query, and checks of resources used by a query during execution.
  // The two errors are raised in different points in the code, but should
  // look identical to users (and tests).
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  private static boolean initialized;
  private static BasicAuthHelper authHelper;
  private static HttpClient druid99;

  @BeforeClass
  @AfterClass
  public static void resetTestState()
  {
    initialized = false;
    druid99 = null;
    authHelper = null;
  }

  @Before
  public void before() throws Exception
  {
    // Do once per class, but requires state available only when we have
    // the first instance of the test class.
    if (!initialized) {
      // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
      ITRetryUtil.retryUntilTrue(
          () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
      );

      setupHttpClientsAndUsers();
      setExpectedSystemSchemaObjects();
      initialized = true;
    }
  }

  @Test
  public void test_druid99User_hasNodeAccess()
  {
    checkNodeAccess(druid99);
  }

  @Override
  protected void setupDatasourceOnlyUser() throws Exception
  {
    authHelper.createUserAndRoleWithPermissions(
        "datasourceOnlyUser",
        "helloworld",
        "datasourceOnlyRole",
        DATASOURCE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndContextParamsUser() throws Exception
  {
    authHelper.createUserAndRoleWithPermissions(
        "datasourceAndContextParamsUser",
        "helloworld",
        "datasourceAndContextParamsRole",
        DATASOURCE_QUERY_CONTEXT_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysTableUser() throws Exception
  {
    authHelper.createUserAndRoleWithPermissions(
        "datasourceAndSysUser",
        "helloworld",
        "datasourceAndSysRole",
        DATASOURCE_SYS_PERMISSIONS
    );
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser() throws Exception
  {
    authHelper.createUserAndRoleWithPermissions(
        "datasourceWithStateUser",
        "helloworld",
        "datasourceWithStateRole",
        DATASOURCE_SYS_STATE_PERMISSIONS
    );
  }

  @Override
  protected void setupSysTableAndStateOnlyUser() throws Exception
  {
    authHelper.createUserAndRoleWithPermissions(
        "stateOnlyUser",
        "helloworld",
        "stateOnlyRole",
        STATE_ONLY_PERMISSIONS
    );
  }

  @Override
  protected void setupTestSpecificHttpClients() throws Exception
  {
    authHelper = new BasicAuthHelper(
        getHttpClient(User.ADMIN),
        config.getCoordinatorUrl(),
        jsonMapper
    );

    // Clean up from previous run
    LOG.info("Cleaning up any previous users and roles");
    authHelper.cleanupUsersAndRoles();

    // create a new user+role that can read /status
    authHelper.createUserAndRoleWithPermissions(
        "druid",
        "helloworld",
        "druidrole",
        STATE_ONLY_PERMISSIONS
    );

    // create 100 users
    for (int i = 0; i < 100; i++) {
      String user = "druid" + i;
      authHelper.createAuthenticationUser(user);
      authHelper.createAuthorizationUser(user);
    }
    LOG.info("Created 100 users.");

    // setup the last of 100 users and check that it works
    authHelper.setPassword("druid99", "helloworld");
    authHelper.assignRoleToUser("druidrole", "druid99");

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
}
