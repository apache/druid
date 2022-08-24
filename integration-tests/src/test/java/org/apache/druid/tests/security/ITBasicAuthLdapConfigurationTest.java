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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Test(groups = TestNGGroup.LDAP_SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthLdapConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthLdapConfigurationTest.class);

  private static final String LDAP_AUTHENTICATOR = "ldap";
  private static final String LDAP_AUTHORIZER = "ldapauth";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: User LDAP authentication failed.";
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: Unauthorized";

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;


  private HttpClient druidUserClient;
  private HttpClient stateOnlyNoLdapGroupUserClient;

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
  public void test_systemSchemaAccess_stateOnlyNoLdapGroupUser() throws Exception
  {
    HttpUtil.makeRequest(getHttpClient(User.STATE_ONLY_USER), HttpMethod.GET, config.getBrokerUrl() + "/status", null);

    // as user that can only read STATE
    LOG.info("Checking sys.segments query as stateOnlyNoLdapGroupUser...");
    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.servers query as stateOnlyNoLdapGroupUser...");
    verifySystemSchemaServerQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    LOG.info("Checking sys.server_segments query as stateOnlyNoLdapGroupUser...");
    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.tasks query as stateOnlyNoLdapGroupUser...");
    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        Collections.emptyList()
    );
  }

  @Test
  public void test_druidUser_hasNodeAccess()
  {
    checkNodeAccess(druidUserClient);
  }


  @Override
  protected void setupDatasourceOnlyUser() throws Exception
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceOnlyGroup",
        ImmutableMap.of("datasourceOnlyRole", DATASOURCE_ONLY_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndContextParamsUser() throws Exception
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceAndContextParamsGroup",
        ImmutableMap.of("datasourceAndContextParamsRole", DATASOURCE_QUERY_CONTEXT_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndSysTableUser() throws Exception
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceWithSysGroup",
        ImmutableMap.of("datasourceWithSysRole", DATASOURCE_SYS_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser() throws Exception
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceWithStateGroup",
        ImmutableMap.of("datasourceWithStateRole", DATASOURCE_SYS_STATE_PERMISSIONS)
    );
  }

  @Override
  protected void setupSysTableAndStateOnlyUser() throws Exception
  {
    createRoleWithPermissionsAndGroupMapping(
        "stateOnlyGroup",
        ImmutableMap.of("stateOnlyRole", STATE_ONLY_PERMISSIONS)
    );

    // create a role that can read /status
    createRoleWithPermissionsAndGroupMapping(
        "druidGroup",
        ImmutableMap.of("druidrole", STATE_ONLY_PERMISSIONS)
    );

    assignUserToRole("stateOnlyNoLdapGroup", "stateOnlyRole");
  }

  @Override
  protected void setupTestSpecificHttpClients()
  {
    druidUserClient = new CredentialedHttpClient(
        new BasicCredentials("druid", "helloworld"),
        httpClient
    );

    stateOnlyNoLdapGroupUserClient = new CredentialedHttpClient(
        new BasicCredentials("stateOnlyNoLdapGroup", "helloworld"),
        httpClient
    );
  }

  @Override
  protected String getAuthenticatorName()
  {
    return LDAP_AUTHENTICATOR;
  }

  @Override
  protected String getAuthorizerName()
  {
    return LDAP_AUTHORIZER;
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

  private void createRoleWithPermissionsAndGroupMapping(
      String group,
      Map<String, List<ResourceAction>> roleTopermissions
  ) throws Exception
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    roleTopermissions.keySet().forEach(role -> HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null
    ));

    for (Map.Entry<String, List<ResourceAction>> entry : roleTopermissions.entrySet()) {
      String role = entry.getKey();
      List<ResourceAction> permissions = entry.getValue();
      byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          StringUtils.format(
              "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s/permissions",
              config.getCoordinatorUrl(),
              role
          ),
          permissionsBytes
      );
    }

    String groupMappingName = StringUtils.format("%sMapping", group);
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping(
        groupMappingName,
        StringUtils.format("cn=%s,ou=Groups,dc=example,dc=org", group),
        roleTopermissions.keySet()
    );
    byte[] groupMappingBytes = jsonMapper.writeValueAsBytes(groupMapping);
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/%s",
            config.getCoordinatorUrl(),
            groupMappingName
        ),
        groupMappingBytes
    );
  }

  private void assignUserToRole(
      String user,
      String role
  )
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/users/%s",
            config.getCoordinatorUrl(),
            user
        ),
        null
    );

    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/users/%s/roles/%s",
            config.getCoordinatorUrl(),
            user,
            role
        ),
        null
    );
  }
}
