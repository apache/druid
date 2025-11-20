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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class BasicAuthLdapConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final String LDAP_AUTHENTICATOR = "ldap";
  private static final String LDAP_AUTHORIZER = "ldapauth";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  private HttpClient druidUserClient;
  private HttpClient stateOnlyNoLdapGroupUserClient;

  @Override
  protected EmbeddedResource getAuthResource()
  {
    return new LdapAuthResource();
  }

  @Test
  public void test_systemSchemaAccess_stateOnlyNoLdapGroupUser()
  {
    HttpUtil.makeRequest(getHttpClient(User.STATE_ONLY_USER), HttpMethod.GET, getBrokerUrl() + "/status");

    // as user that can only read STATE
    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        "segment_id,num_rows,size"
    );

    verifySystemSchemaServerQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        "server,segment_id"
    );

    verifySystemSchemaQuery(
        stateOnlyNoLdapGroupUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        "task_id,group_id,type,datasource,status,location"
    );
  }

  @Test
  public void test_druidUser_hasNodeAccess()
  {
    checkNodeAccess(druidUserClient);
  }


  @Override
  protected void setupDatasourceOnlyUser()
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceOnlyGroup",
        ImmutableMap.of("datasourceOnlyRole", DATASOURCE_ONLY_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndContextParamsUser()
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceAndContextParamsGroup",
        ImmutableMap.of("datasourceAndContextParamsRole", DATASOURCE_QUERY_CONTEXT_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndSysTableUser()
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceWithSysGroup",
        ImmutableMap.of("datasourceWithSysRole", DATASOURCE_SYS_PERMISSIONS)
    );
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser()
  {
    createRoleWithPermissionsAndGroupMapping(
        "datasourceWithStateGroup",
        ImmutableMap.of("datasourceWithStateRole", DATASOURCE_SYS_STATE_PERMISSIONS)
    );
  }

  @Override
  protected void setupSysTableAndStateOnlyUser()
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
  )
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    roleTopermissions.keySet().forEach(role -> HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s",
            getCoordinatorUrl(),
            role
        )
    ));

    for (Map.Entry<String, List<ResourceAction>> entry : roleTopermissions.entrySet()) {
      String role = entry.getKey();
      List<ResourceAction> permissions = entry.getValue();
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          StringUtils.format(
              "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s/permissions",
              getCoordinatorUrl(),
              role
          ),
          permissions,
          HttpResponseStatus.OK
      );
    }

    String groupMappingName = StringUtils.format("%sMapping", group);
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping(
        groupMappingName,
        StringUtils.format("cn=%s,ou=Groups,dc=example,dc=org", group),
        roleTopermissions.keySet()
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/%s",
            getCoordinatorUrl(),
            groupMappingName
        ),
        groupMapping,
        HttpResponseStatus.OK
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
            getCoordinatorUrl(),
            user
        )
    );

    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/users/%s/roles/%s",
            getCoordinatorUrl(),
            user,
            role
        )
    );
  }
}
