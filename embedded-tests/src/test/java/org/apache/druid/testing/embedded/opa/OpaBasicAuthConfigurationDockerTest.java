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

package org.apache.druid.testing.embedded.opa;

import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.Access;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.apache.druid.testing.embedded.auth.AbstractAuthConfigurationTest;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Tag;

import java.util.Properties;

@Tag("docker-test")
public class OpaBasicAuthConfigurationDockerTest extends AbstractAuthConfigurationTest
{
  private static final String AUTHENTICATOR_NAME = "basic";
  private static final String AUTHORIZER_NAME = "opaauth";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: " + Access.DEFAULT_ERROR_MESSAGE;

  @Override
  protected void setupDatasourceOnlyUser()
  {
    createUser("datasourceOnlyUser", "helloworld");
  }

  @Override
  protected void setupDatasourceAndContextParamsUser()
  {
    createUser("datasourceAndContextParamsUser", "helloworld");
  }

  @Override
  protected void setupDatasourceAndSysTableUser()
  {
    createUser("datasourceAndSysUser", "helloworld");
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser()
  {
    createUser("datasourceWithStateUser", "helloworld");
  }

  @Override
  protected void setupSysTableAndStateOnlyUser()
  {
    createUser("stateOnlyUser", "helloworld");
  }

  @Override
  protected void setupTestSpecificHttpClients()
  {
    // No test specific clients needed for this basic happy path.
  }

  @Override
  protected String getAuthenticatorName()
  {
    return AUTHENTICATOR_NAME;
  }

  @Override
  protected String getAuthorizerName()
  {
    return AUTHORIZER_NAME;
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
  protected EmbeddedResource getAuthResource()
  {
    return new OpaBasicAuthResource();
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForInvalidAdmin()
  {
    Properties properties = new Properties();
    properties.setProperty("user", "admin");
    properties.setProperty("password", "invalid_password");
    return properties;
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForUser(User user)
  {
    Properties properties = new Properties();
    properties.setProperty("user", user.getName());
    properties.setProperty("password", user.getPassword());
    return properties;
  }

  private void createUser(String username, String password)
  {
    String baseUrl = getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/" + username;
    HttpUtil.makeRequest(getHttpClient(User.ADMIN), HttpMethod.POST, baseUrl, null, HttpResponseStatus.OK);
    HttpUtil.makeRequest(getHttpClient(User.ADMIN), HttpMethod.POST, baseUrl + "/credentials", new BasicAuthenticatorCredentialUpdate(password, 5000), HttpResponseStatus.OK);
  }
}
