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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Test(groups = TestNGGroup.LDAP_SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthLdapConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthLdapConfigurationTest.class);

  private static final String LDAP_AUTHENTICATOR = "ldap";
  private static final String LDAP_AUTHORIZER = "ldapauth";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: BasicSecurityAuthenticationException: User LDAP authentication failed.";

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  private HttpClient druidUserClient;
  private HttpClient stateOnlyNoLdapGroupUserClient;

  @BeforeClass
  public void before() throws Exception
  {
    // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
    );

    setupHttpClients();
    setupUsers();
    setExpectedSystemSchemaObjects();
  }

  @Test
  public void test_systemSchemaAccess_admin() throws Exception
  {
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
    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceOnlyUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/druid/v2/datasources/auth_test",
        null
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceOnlyUser...");
    verifySystemSchemaQuery(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments.stream()
                     .filter((segmentEntry) -> "auth_test".equals(segmentEntry.get("datasource")))
                     .collect(Collectors.toList())
    );

    LOG.info("Checking sys.servers query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.server_segments query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.tasks query as datasourceOnlyUser...");
    verifySystemSchemaQuery(
        datasourceOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks.stream()
                  .filter((taskEntry) -> "auth_test".equals(taskEntry.get("datasource")))
                  .collect(Collectors.toList())
    );
  }

  @Test
  public void test_systemSchemaAccess_datasourceWithStateUser() throws Exception
  {
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
  public void test_systemSchemaAccess_stateOnlyNoLdapGroupUser() throws Exception
  {
    HttpUtil.makeRequest(stateOnlyUserClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);

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
  public void test_unsecuredPathWithoutCredentials_allowed()
  {
    // check that we are allowed to access unsecured path without credentials.
    checkUnsecuredCoordinatorLoadQueuePath(httpClient);
  }

  @Test
  public void test_admin_loadStatus() throws Exception
  {
    checkLoadStatus(adminClient);
  }

  @Test
  public void test_admin_hasNodeAccess()
  {
    checkNodeAccess(adminClient);
  }

  @Test
  public void test_internalSystemUser_hasNodeAccess()
  {
    checkNodeAccess(internalSystemClient);
  }

  @Test
  public void test_druidUser_hasNodeAccess()
  {
    checkNodeAccess(druidUserClient);
  }

  @Test
  public void test_avaticaQuery_broker()
  {
    testAvaticaQuery(getBrokerAvacticaUrl());
  }

  @Test
  public void test_avaticaQuery_router()
  {
    testAvaticaQuery(getRouterAvacticaUrl());
  }

  @Test
  public void test_avaticaQueryAuthFailure_broker() throws Exception
  {
    testAvaticaAuthFailure(getBrokerAvacticaUrl());
  }

  @Test
  public void test_avaticaQueryAuthFailure_router() throws Exception
  {
    testAvaticaAuthFailure(getRouterAvacticaUrl());
  }

  @Test
  public void test_admin_optionsRequest()
  {
    verifyAdminOptionsRequest();
  }

  @Test
  public void test_authentication_invalidAuthName_fails()
  {
    verifyAuthenticatioInvalidAuthNameFails();
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

  @Override
  void setupUsers() throws Exception
  {
    // create a role that can only read 'auth_test'
    List<ResourceAction> readDatasourceOnlyPermissions = Collections.singletonList(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "datasourceOnlyGroup",
        ImmutableMap.of("datasourceOnlyRole", readDatasourceOnlyPermissions)
    );

    // create a new role that can only read 'auth_test' + STATE read access
    List<ResourceAction> readDatasourceWithStatePermissions = ImmutableList.of(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "datasourceWithStateGroup",
        ImmutableMap.of("datasourceWithStateRole", readDatasourceWithStatePermissions)
    );

    // create a new role with only STATE read access
    List<ResourceAction> stateOnlyPermissions = ImmutableList.of(
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "stateOnlyGroup",
        ImmutableMap.of("stateOnlyRole", stateOnlyPermissions)
    );

    // create a role that can read /status
    createRoleWithPermissionsAndGroupMapping(
        "druidGroup",
        ImmutableMap.of("druidrole", stateOnlyPermissions)
    );

    assignUserToRole("stateOnlyNoLdapGroup", "stateOnlyRole");
  }

  @Override
  void setupTestSpecificHttpClients()
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

  private void createRoleWithPermissionsAndGroupMapping(
      String group,
      Map<String, List<ResourceAction>> roleTopermissions
  ) throws Exception
  {
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

  @Override
  String getAuthenticatorName()
  {
    return LDAP_AUTHENTICATOR;
  }

  @Override
  String getAuthorizerName()
  {
    return LDAP_AUTHORIZER;
  }

  @Override
  String getExpectedAvaticaAuthError()
  {
    return EXPECTED_AVATICA_AUTH_ERROR;
  }
}
