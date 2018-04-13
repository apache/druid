/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.tests.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Client;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.CredentialedHttpClient;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.auth.BasicCredentials;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import io.druid.sql.avatica.DruidAvaticaHandler;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthConfigurationTest.class);

  private static final TypeReference LOAD_STATUS_TYPE_REFERENCE =
      new TypeReference<Map<String, Boolean>>()
      {
      };

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @Client
  HttpClient httpClient;

  StatusResponseHandler responseHandler = new StatusResponseHandler(StandardCharsets.UTF_8);

  @Test
  public void testAuthConfiguration() throws Exception
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    HttpClient internalSystemClient = new CredentialedHttpClient(
        new BasicCredentials("druid_system", "warlock"),
        httpClient
    );

    HttpClient newUserClient = new CredentialedHttpClient(
        new BasicCredentials("druid", "helloworld"),
        httpClient
    );

    final HttpClient unsecuredClient = httpClient;

    // check that we are allowed to access unsecured path without credentials.
    checkUnsecuredCoordinatorLoadQueuePath(unsecuredClient);

    // check that admin works
    checkNodeAccess(adminClient);

    // check that internal user works
    checkNodeAccess(internalSystemClient);

    // create a new user that can read /status
    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid/credentials",
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate("helloworld", 5000))
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/roles/druidrole",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid/roles/druidrole",
        null
    );

    List<ResourceAction> permissions = Arrays.asList(
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );
    byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/roles/druidrole/permissions",
        permissionsBytes
    );

    // check that the new user works
    checkNodeAccess(newUserClient);

    // check loadStatus
    checkLoadStatus(adminClient);


    // create 100 users
    for (int i = 0; i < 100; i++) {
      makeRequest(
          adminClient,
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid" + i,
          null
      );

      makeRequest(
          adminClient,
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid" + i,
          null
      );

      LOG.info("Finished creating user druid" + i);
    }

    // setup the last of 100 users and check that it works
    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid99/credentials",
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate("helloworld", 5000))
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid99/roles/druidrole",
        null
    );

    HttpClient newUser99Client = new CredentialedHttpClient(
        new BasicCredentials("druid99", "helloworld"),
        httpClient
    );

    LOG.info("Checking access for user druid99.");
    checkNodeAccess(newUser99Client);

    String brokerUrl = "jdbc:avatica:remote:url=" + config.getBrokerUrl() + DruidAvaticaHandler.AVATICA_PATH;
    String routerUrl = "jdbc:avatica:remote:url=" + config.getRouterUrl() + DruidAvaticaHandler.AVATICA_PATH;

    LOG.info("Checking Avatica query on broker.");
    testAvaticaQuery(brokerUrl);

    LOG.info("Checking Avatica query on router.");
    testAvaticaQuery(routerUrl);

    LOG.info("Testing Avatica query on broker with incorrect credentials.");
    testAvaticaAuthFailure(brokerUrl);
    
    LOG.info("Testing Avatica query on router with incorrect credentials.");
    testAvaticaAuthFailure(routerUrl);
  }

  private void checkUnsecuredCoordinatorLoadQueuePath(HttpClient client)
  {
    makeRequest(client, HttpMethod.GET, config.getCoordinatorUrl() + "/druid/coordinator/v1/loadqueue", null);
  }

  private void testAvaticaQuery(String url)
  {
    LOG.info("URL: " + url);
    try {
      Properties connectionProperties = new Properties();
      connectionProperties.put("user", "admin");
      connectionProperties.put("password", "priest");
      Connection connection = DriverManager.getConnection(url, connectionProperties);
      Statement statement = connection.createStatement();
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      ResultSet resultSet = statement.executeQuery(query);
      Assert.assertTrue(resultSet.next());
      statement.close();
      connection.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testAvaticaAuthFailure(String url) throws Exception
  {
    LOG.info("URL: " + url);
    try {
      Properties connectionProperties = new Properties();
      connectionProperties.put("user", "admin");
      connectionProperties.put("password", "wrongpassword");
      Connection connection = DriverManager.getConnection(url, connectionProperties);
      Statement statement = connection.createStatement();
      statement.setMaxRows(450);
      String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
      statement.executeQuery(query);
    }
    catch (AvaticaSqlException ase) {
      Assert.assertEquals(
          ase.getErrorMessage(),
          "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: ForbiddenException: Authentication failed."
      );
      return;
    }
    Assert.fail("Test failed, did not get AvaticaSqlException.");
  }


  private void checkNodeAccess(HttpClient httpClient)
  {
    makeRequest(httpClient, HttpMethod.GET, config.getCoordinatorUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getIndexerUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getHistoricalUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getRouterUrl() + "/status", null);
  }

  private void checkLoadStatus(HttpClient httpClient) throws Exception
  {
    checkLoadStatusSingle(httpClient, config.getCoordinatorUrl());
    checkLoadStatusSingle(httpClient, config.getIndexerUrl());
    checkLoadStatusSingle(httpClient, config.getBrokerUrl());
    checkLoadStatusSingle(httpClient, config.getHistoricalUrl());
    checkLoadStatusSingle(httpClient, config.getRouterUrl());
  }

  private void checkLoadStatusSingle(HttpClient httpClient, String baseUrl) throws Exception
  {
    StatusResponseHolder holder = makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authentication/loadStatus",
        null
    );
    String content = holder.getContent();
    Map<String, Boolean> loadStatus = jsonMapper.readValue(content, LOAD_STATUS_TYPE_REFERENCE);

    Assert.assertNotNull(loadStatus.get("basic"));
    Assert.assertTrue(loadStatus.get("basic"));

    holder = makeRequest(
        httpClient,
        HttpMethod.GET,
        baseUrl + "/druid-ext/basic-security/authorization/loadStatus",
        null
    );
    content = holder.getContent();
    loadStatus = jsonMapper.readValue(content, LOAD_STATUS_TYPE_REFERENCE);

    Assert.assertNotNull(loadStatus.get("basic"));
    Assert.assertTrue(loadStatus.get("basic"));
  }

  private StatusResponseHolder makeRequest(HttpClient httpClient, HttpMethod method, String url, byte[] content)
  {
    try {
      Request request = new Request(method, new URL(url));
      if (content != null) {
        request.setContent(MediaType.APPLICATION_JSON, content);
      }
      int retryCount = 0;

      StatusResponseHolder response;

      while (true) {
        response = httpClient.go(
            request,
            responseHandler
        ).get();

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          String errMsg = StringUtils.format(
              "Error while making request to url[%s] status[%s] content[%s]",
              url,
              response.getStatus(),
              response.getContent()
          );
          // it can take time for the auth config to propagate, so we retry
          if (retryCount > 4) {
            throw new ISE(errMsg);
          } else {
            LOG.error(errMsg);
            LOG.error("retrying in 3000ms, retryCount: " + retryCount);
            retryCount++;
            Thread.sleep(3000);
          }
        } else {
          break;
        }
      }
      return response;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
